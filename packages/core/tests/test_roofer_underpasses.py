"""Roofer receives per-tile underpasses, including NULL-height fallbacks."""

import tomllib
from pathlib import Path
from typing import TypedDict
from unittest.mock import MagicMock

import pytest
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.values import NlTransform
from bag3d.common.types import PostgresTableIdentifier
from dagster import AssetExecutionContext, AssetKey
from dagster._core.definitions.decorators.op_decorator import DecoratedOpFunction

from bag3d.core.assets.reconstruction import reconstruction as module


class RooferInputs(TypedDict):
    context: MagicMock
    computation_db: MagicMock
    file_store: FileStoreResource
    nl_transform: NlTransform
    reconstruction_input: PostgresTableIdentifier
    index: PostgresTableIdentifier
    tiles: PostgresTableIdentifier
    metadata_ahn3: PostgresTableIdentifier
    metadata_ahn4: PostgresTableIdentifier
    metadata_ahn5: PostgresTableIdentifier
    metadata_ahn6: PostgresTableIdentifier
    underpass_heights: PostgresTableIdentifier


def inputs(tmp_path: Path) -> RooferInputs:
    database = MagicMock(
        host="localhost",
        port=5432,
        dbname="bag",
        user="tester",
        password="secret",
        other_params={"sslmode": "require"},
    )
    database.connection.get_dict.return_value = [{"filename": "/data/ahn.laz"}]
    return RooferInputs(
        context=MagicMock(spec=AssetExecutionContext, partition_key="10/1/2"),
        computation_db=database,
        file_store=FileStoreResource(root_dir=str(tmp_path)),
        nl_transform=NlTransform(scale=[0.001] * 3, translate=[0.0] * 3),
        reconstruction_input=PostgresTableIdentifier("input", "buildings"),
        index=PostgresTableIdentifier("input", "index"),
        tiles=PostgresTableIdentifier("input", "tiles"),
        metadata_ahn3=PostgresTableIdentifier("ahn", "ahn3"),
        metadata_ahn4=PostgresTableIdentifier("ahn", "ahn4"),
        metadata_ahn5=PostgresTableIdentifier("ahn", "ahn5"),
        metadata_ahn6=PostgresTableIdentifier("ahn", "ahn6"),
        underpass_heights=PostgresTableIdentifier("underpasses", "heights"),
    )


def test_roofer_config_and_tile_selection(tmp_path):
    args = inputs(tmp_path)
    path, _, _ = module.create_roofer_config(**args)
    config = tomllib.loads(path.read_text())
    assert config["underpass-default-height"] == 2.5
    assert config["underpass-height-attribute"] == "underpass_candidate_elevations"
    assert config["underpass-id-attribute"] == "identificatie"
    assert "u_10/1/2" in config["underpass-source"]
    assert "(geom)" in config["underpass-source"]
    assert "secret" not in path.read_text()
    calls = args["computation_db"].connection.send_query.call_args_list
    query = calls[-1].args[0].as_string()
    assert "SELECT h.*" in query  # Preserve holes and multiple polygons per building.
    assert "f.identificatie = h.identificatie" in query
    assert "IS NOT NULL" not in query  # Roofer owns the per-polygon fallback.
    assert module.roofer_environment(args["computation_db"])["PGPASSWORD"] == "secret"


@pytest.mark.parametrize("drop_views", [True, False])
def test_views_cleaned_after_roofer_failure(tmp_path, monkeypatch, drop_views):
    args = inputs(tmp_path)
    roofer = MagicMock()
    roofer.runner.run.side_effect = RuntimeError("roofer failed")
    monkeypatch.setattr(
        module,
        "create_roofer_config",
        lambda *args, **kwargs: (
            tmp_path / "roofer.toml",
            tmp_path,
            PostgresTableIdentifier("input", "t_10/1/2"),
        ),
    )
    compute_fn = module.reconstructed_building_models.op.compute_fn
    assert isinstance(compute_fn, DecoratedOpFunction)
    function = compute_fn.decorated_fn
    with pytest.raises(RuntimeError, match="roofer failed"):
        function(
            context=args["context"],
            config=module.RooferConfig(drop_views=drop_views),
            computation_db=args["computation_db"],
            roofer=roofer,
            file_store=args["file_store"],
            nl_transform=args["nl_transform"],
            tiles=args["tiles"],
            index=args["index"],
            reconstruction_input=args["reconstruction_input"],
            **{f"metadata_ahn{n}_index": args[f"metadata_ahn{n}"] for n in range(3, 7)},
            underpass_heights=args["underpass_heights"],
        )
    db = args["computation_db"].connection
    assert db.send_query.call_count == int(drop_views)
    if drop_views:
        parameters = db.send_query.call_args.kwargs["query_params"]
        assert str(parameters["underpass_view"]) == "input.u_10/1/2"
        assert str(parameters["tile_view"]) == "input.t_10/1/2"


def test_reconstruction_depends_on_unpartitioned_underpass_heights():
    asset = module.reconstructed_building_models
    assert AssetKey(["underpasses", "heights"]) in asset.asset_deps[asset.key]
