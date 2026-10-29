"""End-to-end tests using isolated schemas in an explicitly supplied test database."""

import os
from uuid import uuid4

import pytest
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.types import PostgresTableIdentifier
from dagster import AssetSelection, AssetSpec, DagsterInstance, asset, materialize
from psycopg import connect
from psycopg.conninfo import conninfo_to_dict
from psycopg.sql import SQL, Identifier
from shapely import from_wkb, to_wkb
from shapely.geometry import box

from bag3d.underpasses import workflow
from bag3d.underpasses.assets import underpasses
from bag3d.underpasses.code_location import all_assets

pytestmark = pytest.mark.integration


@pytest.fixture
def database_fixture(monkeypatch):
    dsn = os.getenv("BAG3D_UNDERPASSES_TEST_DSN")
    if not dsn:
        pytest.skip("Set BAG3D_UNDERPASSES_TEST_DSN to a disposable PostGIS database")
    schema = f"underpasses_test_{uuid4().hex}"
    bag = PostgresTableIdentifier(schema, "bag_input")
    bgt = PostgresTableIdentifier(schema, "bgt_input")
    monkeypatch.setattr(underpasses, "SCHEMA", schema)
    params = {key: str(value) for key, value in conninfo_to_dict(dsn).items()}
    resource = DatabaseResource(
        host=params.pop("host", "localhost"),
        port=int(params.pop("port", "5432")),
        dbname=params.pop("dbname"),
        user=params.pop("user"),
        password=params.pop("password", None),
        other_params=params or None,
    )
    ids = [f"NL.IMBAG.Pand.{i:016d}" for i in range(1, 6)]
    try:
        with connect(dsn) as conn:
            conn.execute(SQL("CREATE SCHEMA {}").format(Identifier(schema)))
            conn.execute(
                SQL("""
                CREATE TABLE {} (
                    identificatie text PRIMARY KEY,
                    geometrie geometry(MultiPolygon, 28992)
                )
            """).format(bag.id)
            )
            conn.execute(
                SQL("""
                CREATE TABLE {} (
                    identificatiebagpnd text,
                    relatievehoogteligging integer,
                    geometrie geometry(MultiPolygon, 28992)
                )
            """).format(bgt.id)
            )
            for i, identificatie in enumerate(ids):
                conn.execute(
                    SQL(
                        "INSERT INTO {} VALUES (%s, ST_Multi(ST_GeomFromWKB(%s, 28992)))"
                    ).format(bag.id),
                    (identificatie, to_wkb(box(i * 20, 0, i * 20 + 10, 10))),
                )
            # The first building has three BGT pieces, including an underground
            # piece. The detector must union all three and leave x=6..10.
            pieces = [
                (ids[0], 0, box(0, 0, 4, 5)),
                (ids[0], 0, box(0, 5, 4, 10)),
                (ids[0], -1, box(4, 0, 6, 10)),
                (ids[0], 0, box(1000, 1000, 1010, 1010)),  # fails bbox test
                (ids[1], 0, box(20, 0, 30, 10)),  # full coverage
                # ids[2] has no matching BGT and must not enter the pipeline.
                (ids[3], 0, box(60, 0, 69.9, 10)),  # narrow sliver
                (ids[4], 0, box(84, 0, 86, 10)),  # two disconnected candidates
            ]
            for identificatie, level, geometry in pieces:
                conn.execute(
                    SQL("""INSERT INTO {} VALUES (
                        %s, %s, ST_Multi(ST_GeomFromWKB(%s, 28992)))
                    """).format(bgt.id),
                    (identificatie[14:], level, to_wkb(geometry)),
                )
        yield dsn, resource, schema, bag, bgt, ids
    finally:
        with connect(dsn) as conn:
            conn.execute(
                SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(schema))
            )


def _rows(dsn, table):
    with connect(dsn) as conn:
        return conn.execute(
            SQL("""
            SELECT underpass_id, identificatie, ST_AsBinary(geom)
            FROM {} ORDER BY underpass_id
        """).format(table.id)
        ).fetchall()


def test_full_workflow_cross_location_inputs_reruns_and_ids(database_fixture, tmp_path):
    dsn, resource, schema, bag, bgt, ids = database_fixture

    @asset(key=["bag", "bag_pandactueelbestaand"])
    def source_bag() -> PostgresTableIdentifier:
        return bag

    @asset(key=["bgt", "bgt_pandactueelbestaand"])
    def source_bgt() -> PostgresTableIdentifier:
        return bgt

    sources = [
        AssetSpec(source_bag.key),
        AssetSpec(source_bgt.key),
        AssetSpec(["intermediary", "bag_adjacency"]),
    ]
    target = PostgresTableIdentifier(schema, "geometries")
    with DagsterInstance.local_temp(str(tmp_path)) as instance:
        # Materialize sources separately: underpasses loads their identifiers
        # through the instance filesystem IO manager, as separate locations do.
        assert materialize([source_bag, source_bgt], instance=instance).success
        baseline = None
        for chunk_size, concurrency in ((1, 1), (2, 2), (1000, 4)):
            config = {
                "ops": {
                    stage: {
                        "config": {
                            "chunk_size": chunk_size,
                            "concurrency": concurrency,
                        }
                    }
                    for stage in ("bag_minus_bgt", "snapped_differences", "geometries")
                }
            }
            result = materialize(
                [*all_assets, *sources],
                instance=instance,
                resources={"computation_db": resource},
                selection=AssetSelection.assets(
                    *[
                        ["underpasses", name]
                        for name in (
                            "bag_bgt_join",
                            "bag_minus_bgt",
                            "snapped_differences",
                            "geometries",
                        )
                    ]
                ),
                run_config=config,
            )
            assert result.success
            rows = _rows(dsn, target)
            assert len(rows) == 3
            assert [row[0] for row in rows] == [1, 2, 3]
            assert rows[0][1] == ids[0]
            assert from_wkb(rows[0][2]).equals(box(6, 0, 10, 10))
            assert [row[1] for row in rows[1:]] == [ids[4], ids[4]]
            if baseline is None:
                baseline = rows
            assert rows == baseline
        # Selecting a single stage loads its existing upstream table identifier.
        assert materialize(
            [*all_assets, *sources],
            instance=instance,
            resources={"computation_db": resource},
            selection=AssetSelection.assets(["underpasses", "geometries"]),
        ).success
        assert _rows(dsn, target) == baseline
    with connect(dsn) as conn:
        for table, count in (
            ("bag_bgt_join", 4),
            ("bag_minus_bgt", 2),
            ("snapped_differences", 2),
        ):
            row = conn.execute(
                SQL("SELECT count(*) FROM {}").format(Identifier(schema, table))
            ).fetchone()
            assert row is not None and row[0] == count
        assert conn.execute(
            SQL("SELECT DISTINCT ST_GeometryType(geom), ST_SRID(geom) FROM {}").format(
                target.id
            )
        ).fetchall() == [("ST_Polygon", 28992)]
        indexes = conn.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname=%s AND tablename='geometries'",
            (schema,),
        ).fetchall()
        assert len(indexes) == 3
        assert any("USING gist" in row[0] for row in indexes)


def test_empty_result_and_failed_replacement_rolls_back(database_fixture, monkeypatch):
    dsn, _, schema, _, _, _ = database_fixture
    source = PostgresTableIdentifier(schema, "snapped")
    target = PostgresTableIdentifier(schema, "geometries")
    with connect(dsn) as conn:
        conn.execute(
            SQL(
                "CREATE TABLE {} (identificatie text, geom geometry(MultiPolygon, 28992))"
            ).format(source.id)
        )
    options = underpasses.UnderpassesConfig(concurrency=1, chunk_size=1).model_dump()
    stats = workflow.run_geometry_stage(dsn, "geometries", source, target, **options)
    assert stats == {"processed": 0, "written": 0}
    assert _rows(dsn, target) == []
    with connect(dsn) as conn:
        conn.execute(
            SQL(
                "INSERT INTO {} VALUES ('building', ST_Multi(ST_MakeEnvelope(0,0,10,10,28992)))"
            ).format(source.id)
        )
    workflow.run_geometry_stage(dsn, "geometries", source, target, **options)
    baseline = _rows(dsn, target)

    def fail(*args, **kwargs):
        raise RuntimeError("injected worker failure")

    monkeypatch.setattr(workflow, "process_chunk", fail)
    with pytest.raises(RuntimeError, match="injected worker failure"):
        workflow.run_geometry_stage(dsn, "geometries", source, target, **options)
    assert _rows(dsn, target) == baseline
