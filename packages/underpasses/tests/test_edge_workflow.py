"""Stage-level behavior and paired offset table replacement."""

import pytest
from bag3d.common.types import PostgresTableIdentifier
from dagster import AssetSelection, AssetSpec, DagsterInstance, asset, materialize
from psycopg import connect
from psycopg.sql import SQL
from shapely import from_wkb
from shapely.geometry import LineString, box

from bag3d.underpasses import edge_workflow
from bag3d.underpasses.assets.edges import EdgeOffsetConfig
from bag3d.underpasses.code_location import all_assets

from .test_integration import database_fixture as database_fixture  # noqa: PLC0414 - Register shared fixture.


def test_offset_defaults_and_known_exclusions():
    assert EdgeOffsetConfig().offset_distance == 0.05
    counts, successes, skipped = edge_workflow.offset_chunk(
        [(1, "building", [], [])],
        offset_distance=0.05,
        tolerance=0.001,
    )
    assert counts == 1 and not successes
    assert skipped == [("building", 1, "no_movable_edges")]
    with pytest.raises(ValueError):
        EdgeOffsetConfig(offset_distance=float("nan"))


@pytest.mark.integration
def test_complete_job_and_atomic_offset_outputs(
    database_fixture, tmp_path, monkeypatch
):
    dsn, resource, schema, bag, bgt, ids = database_fixture
    adjacency = PostgresTableIdentifier(schema, "adjacency")
    # BAG touches only y=2..8; BGT touches y=0..10. Shared length must be 10.
    neighbour = "NL.IMBAG.Pand.0000000000000006"
    with connect(dsn) as conn:
        conn.execute(
            SQL(
                "CREATE TABLE {} (identificatie text, adjacent_identificatie text)"
            ).format(adjacency.id)
        )
        conn.execute(
            SQL("INSERT INTO {} VALUES (%s,%s)").format(adjacency.id),
            (ids[0], neighbour),
        )
        conn.execute(
            SQL(
                "INSERT INTO {} VALUES (%s,ST_Multi(ST_MakeEnvelope(10,2,20,8,28992)))"
            ).format(bag.id),
            (neighbour,),
        )
        conn.execute(
            SQL(
                "INSERT INTO {} VALUES (%s,0,ST_Multi(ST_MakeEnvelope(10,0,20,10,28992)))"
            ).format(bgt.id),
            (neighbour[14:],),
        )

    @asset(key=["bag", "bag_pandactueelbestaand"])
    def bag_source():
        return bag

    @asset(key=["bgt", "bgt_pandactueelbestaand"])
    def bgt_source():
        return bgt

    @asset(key=["intermediary", "bag_adjacency"])
    def adjacency_source():
        return adjacency

    sources = [bag_source, bgt_source, adjacency_source]
    specs = [AssetSpec(source.key) for source in sources]
    target = PostgresTableIdentifier(schema, "extended_geometries")
    skipped = PostgresTableIdentifier(schema, "skipped_underpasses")
    edge_table = PostgresTableIdentifier(schema, "edges")
    geometry_table = PostgresTableIdentifier(schema, "geometries")

    def results():
        with connect(dsn) as conn:
            return conn.execute(
                SQL(
                    "SELECT underpass_id, ST_AsBinary(geom) FROM {} ORDER BY underpass_id"
                ).format(target.id)
            ).fetchall()

    with DagsterInstance.local_temp(str(tmp_path)) as instance:
        assert materialize(sources, instance=instance).success
        baseline = None
        for concurrency in (1, 2):
            config = {
                "ops": {
                    name: {"config": {"concurrency": concurrency, "chunk_size": 1}}
                    for name in (
                        "bag_minus_bgt",
                        "snapped_differences",
                        "geometries",
                        "edges",
                        "offset_edges",
                    )
                }
            }
            assert materialize(
                [*all_assets, *specs],
                instance=instance,
                resources={"computation_db": resource},
                run_config=config,
            ).success
            current = results()
            assert len(current) == 3
            assert (
                from_wkb(current[0][1]).hausdorff_distance(box(6, -0.05, 10, 10.05))
                < 1e-9
            )
            if baseline is None:
                baseline = current
            assert current == baseline
        # Selecting either output includes its coupled sibling.
        assert materialize(
            [*all_assets, *specs],
            instance=instance,
            resources={"computation_db": resource},
            selection=AssetSelection.assets(
                ["underpasses", "extended_geometries"]
            ).required_multi_asset_neighbors(),
        ).success
    with connect(dsn) as conn:
        shared = conn.execute(
            SQL(
                "SELECT ST_AsBinary(geom) FROM {} WHERE identificatie=%s AND edge_type='shared'"
            ).format(edge_table.id),
            (ids[0],),
        ).fetchall()
        assert len(shared) == 1
        assert from_wkb(shared[0][0]).equals(LineString([(10, 0), (10, 10)]))
    settings = {"offset_distance": 0.05, "tolerance": 0.001}
    query = edge_workflow.offset_query(geometry_table, edge_table)
    original = edge_workflow.offset_chunk

    def fail(*args, **kwargs):
        raise RuntimeError("unexpected failure")

    monkeypatch.setattr(edge_workflow, "offset_chunk", fail)
    with pytest.raises(RuntimeError, match="unexpected failure"):
        edge_workflow.run_edges(
            dsn,
            query,
            target,
            skipped=skipped,
            concurrency=1,
            chunk_size=1,
            settings=settings,
        )
    assert results() == baseline
    monkeypatch.setattr(edge_workflow, "offset_chunk", original)
    with connect(dsn) as conn:
        conn.execute(SQL("UPDATE {} SET edge_type='interior'").format(edge_table.id))
    stats = edge_workflow.run_edges(
        dsn,
        query,
        target,
        skipped=skipped,
        concurrency=1,
        chunk_size=1,
        settings=settings,
    )
    assert stats["skipped"] == 3 and results() == []
    with connect(dsn) as conn:
        assert (
            len(conn.execute(SQL("SELECT * FROM {}").format(skipped.id)).fetchall())
            == 3
        )
        conn.execute(SQL("DELETE FROM {}").format(geometry_table.id))
    stats = edge_workflow.run_edges(
        dsn,
        query,
        target,
        skipped=skipped,
        concurrency=1,
        chunk_size=1,
        settings=settings,
    )
    assert stats["processed"] == 0
    with connect(dsn) as conn:
        assert conn.execute(SQL("SELECT * FROM {}").format(skipped.id)).fetchall() == []


@pytest.mark.parametrize("distance", [0.0, 0.05, -0.05])
def test_signed_and_zero_offsets(distance):
    from shapely import to_wkb

    geometry = box(0, 0, 4, 4)
    _, rows, skipped = edge_workflow.offset_chunk(
        [(1, "building", ["exterior"], [to_wkb(geometry.boundary)])],
        offset_distance=distance,
        tolerance=0.001,
    )
    assert not skipped and len(rows) == 1
    result = from_wkb(rows[0][3])
    assert result.is_valid
    assert (
        result.hausdorff_distance(box(-distance, -distance, 4 + distance, 4 + distance))
        < 1e-9
    )


@pytest.mark.parametrize(
    "error,reason",
    [
        (edge_workflow.InvalidInputPolygonError("invalid"), "invalid_input_polygon"),
        (edge_workflow.GeometryOffsetError("failed"), "geometry_offset_failed"),
        (ValueError("Polygon boundary segment was not found"), "edge_matching_failed"),
        (ValueError("bad linework"), "value_error"),
    ],
)
def test_known_geometry_failures_are_recorded(monkeypatch, error, reason):
    from shapely import to_wkb

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(edge_workflow, "classify_polygon_from_edge_sets", fail)
    _, rows, skipped = edge_workflow.offset_chunk(
        [(1, "building", ["exterior"], [to_wkb(box(0, 0, 4, 4).boundary)])],
        offset_distance=0.05,
        tolerance=0.001,
    )
    assert not rows and skipped == [("building", 1, reason)]
