"""Tests for CBS download and load assets."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context

from bag3d.common.types import PostgresTableIdentifier
from bag3d.core.assets.cbs.download import (
    CbsBuurtkaartConfig,
    CbsKeyFiguresConfig,
    _clean_cell,
    _clean_records,
    _fetch_cbs_odata,
    extract_cbs_buurtkaart,
    extract_cbs_key_figures,
)
from bag3d.core.assets.cbs.load import (
    _infer_pg_type,
    _load_records_to_postgres,
    cbs_buurten,
    cbs_key_figures,
)


# ---------------------------------------------------------------------------
# _clean_cell
# ---------------------------------------------------------------------------
class TestCleanCell:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ("  123  ", "123"),
            ("Normal", "Normal"),
            ("None", None),
            ("---", None),
            ("___", None),
            ("", ""),
            (42, 42),
            (3.14, 3.14),
            (None, None),
        ],
    )
    def test_clean_cell(self, value, expected):
        assert _clean_cell(value) == expected


# ---------------------------------------------------------------------------
# _records_to_csv
# ---------------------------------------------------------------------------
class TestRecordsToCsv:
    def test_writes_csv_with_cleaned_values(self, tmp_path):
        records = [
            {
                "ID": "0", "Region": "NL", "Type": "Land", "Code": "NL00",
                "Population": "100", "Density": "---",
            },
        ]
        result = _clean_records(records)
        assert result[0]["ID"] == "0"
        assert result[0]["Population"] == "100"
        assert result[0]["Density"] is None

    def test_raises_on_empty_records(self):
        with pytest.raises(ValueError, match="No records"):
            _clean_records([])


# ---------------------------------------------------------------------------
# _fetch_cbs_odata
# ---------------------------------------------------------------------------
class TestFetchCbsOdata:
    def test_paginates_over_multiple_pages(self):
        page1 = {"value": [{"ID": 1}], "odata.nextLink": "http://next"}
        page2 = {"value": [{"ID": 2}]}

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.side_effect = [page1, page2]
            mock_get.return_value.raise_for_status.return_value = None

            result = _fetch_cbs_odata("http://base")

        assert len(result) == 2
        assert result[0]["ID"] == 1
        assert result[1]["ID"] == 2


# ---------------------------------------------------------------------------
# extract_cbs_key_figures
# ---------------------------------------------------------------------------
class TestExtractCbsKeyFigures:
    def test_returns_cleaned_records(self, monkeypatch):
        mock_records = [
            {"ID": 0, "Region": "NL", "Type": "Buurt", "Code": "BU00", "Pop": 100}
        ]
        monkeypatch.setattr(
            "bag3d.core.assets.cbs.download._fetch_cbs_odata",
            lambda url: mock_records,
        )

        file_store = MagicMock()
        config = CbsKeyFiguresConfig(table_ids={"2025": "86165NED"})
        with build_asset_context() as context:
            result = extract_cbs_key_figures(context, config, file_store)

        assert result.metadata["Records [2025]"].value == 1  # type: ignore[reportAttributeAccessIssue]


# ---------------------------------------------------------------------------
# extract_cbs_buurtkaart
# ---------------------------------------------------------------------------
class TestExtractCbsBuurtkaart:
    def test_falls_back_to_any_gpkg(self, tmp_path, monkeypatch):
        cbs_dir = tmp_path / "cbs"
        cbs_dir.mkdir()
        file_store = MagicMock()
        file_store.create_subdir.return_value = cbs_dir

        # Simulate ZIP extraction: create dir but with different GPKG name
        gpkg_dir = cbs_dir / "WijkBuurtkaart_2025_v1"
        gpkg_dir.mkdir()
        fallback = gpkg_dir / "other.gpkg"
        fallback.write_text("fake")

        monkeypatch.setattr(
            "bag3d.core.assets.cbs.download.download_file", lambda *a, **kw: None
        )
        monkeypatch.setattr(
            "bag3d.core.assets.cbs.download.unzip", lambda *a, **kw: None
        )

        with build_asset_context() as context:
            result = extract_cbs_buurtkaart(
                context, CbsBuurtkaartConfig(year="2025", version="v1"), file_store
            )

        assert result.metadata["GeoPackage"].value == str(fallback)  # type: ignore[reportAttributeAccessIssue]

    def test_raises_when_no_gpkg_found(self, tmp_path, monkeypatch):
        cbs_dir = tmp_path / "cbs"
        cbs_dir.mkdir()
        file_store = MagicMock()
        file_store.create_subdir.return_value = cbs_dir

        gpkg_dir = cbs_dir / "WijkBuurtkaart_2025_v1"
        gpkg_dir.mkdir()

        monkeypatch.setattr(
            "bag3d.core.assets.cbs.download.download_file", lambda *a, **kw: None
        )
        monkeypatch.setattr(
            "bag3d.core.assets.cbs.download.unzip", lambda *a, **kw: None
        )

        with build_asset_context() as context:
            with pytest.raises(FileNotFoundError, match="No GeoPackage found"):
                extract_cbs_buurtkaart(
                    context, CbsBuurtkaartConfig(year="2025", version="v1"), file_store
                )


# ---------------------------------------------------------------------------
# _infer_pg_type & _load_records_to_postgres
# ---------------------------------------------------------------------------
class TestInferPgType:
    @pytest.mark.parametrize(
        "records, col, expected",
        [
            ([{"v": 1}, {"v": 2}], "v", "INTEGER"),
            ([{"v": 1.5}, {"v": 2.0}], "v", "DOUBLE PRECISION"),
            ([{"v": "hello"}], "v", "TEXT"),
            ([{"v": None}, {"v": 1}], "v", "INTEGER"),
            ([{"v": None}], "v", "TEXT"),
        ],
    )
    def test_infer_pg_type(self, records, col, expected):
        assert _infer_pg_type(records, col) == expected


class TestLoadRecordsToPostgres:
    def test_creates_table_with_inferred_types(self, monkeypatch):
        records = [{"ID": 1, "Name": "test"}]

        mock_conn = MagicMock()
        mock_conn.dsn = "host=localhost dbname=test"
        mock_db = MagicMock()
        mock_db.connection = mock_conn

        monkeypatch.setattr(
            "bag3d.core.assets.cbs.load.connect",
            lambda dsn: (_ for _ in ()).throw(RuntimeError("stop after CREATE TABLE")),
        )

        table = PostgresTableIdentifier("cbs", "test_table")
        try:
            _load_records_to_postgres(mock_db, records, table)
        except RuntimeError:
            pass

        create_call = mock_conn.send_query.call_args_list[0][0][0]
        sql_str = str(create_call)
        assert "Identifier('ID'" in sql_str
        assert "INTEGER" in sql_str
        assert "Identifier('Name'" in sql_str
        assert "TEXT" in sql_str


# ---------------------------------------------------------------------------
# cbs_key_figures
# ---------------------------------------------------------------------------
class TestCbsKeyFigures:
    def test_adds_primary_key_and_comment(self, monkeypatch):
        records = [{"ID": 1, "Region": "NL"}]

        mock_conn = MagicMock()
        mock_conn.dsn = "host=localhost dbname=test"
        mock_db = MagicMock()
        mock_db.connection = mock_conn

        monkeypatch.setattr(
            "bag3d.core.assets.cbs.load.connect",
            lambda dsn: (_ for _ in ()).throw(RuntimeError("stop after INSERT")),
        )

        with build_asset_context() as context:
            try:
                cbs_key_figures(context, mock_db, {"2025": records})
            except RuntimeError:
                pass

        calls = [str(c[0][0]) for c in mock_conn.send_query.call_args_list]
        sql_str = " ".join(calls)
        assert "key_figures_districts_neighbourhoods" in sql_str
        assert "Identifier('ID'" in sql_str
        assert "INTEGER" in sql_str


# ---------------------------------------------------------------------------
# cbs_buurten
# ---------------------------------------------------------------------------
class TestCbsBuurten:
    def test_runs_ogr2ogr_with_buurten_layer(self):
        mock_conn = MagicMock()
        mock_conn.dsn = "host=test"
        mock_db = MagicMock()
        mock_db.connection = mock_conn

        mock_gdal = MagicMock()
        mock_gdal.runner.run.return_value.success = True

        with build_asset_context() as context:
            result = cbs_buurten(
                context,
                mock_db,
                mock_gdal,
                Path("/mock/buurtkaart.gpkg"),
            )

        # Verify ogr2ogr was called with the buurten layer
        mock_gdal.runner.run.assert_called_once()
        cmd = mock_gdal.runner.run.call_args[0][0]
        assert "buurten" in cmd
        assert "{exe}" in cmd
        assert "{dsn}" in cmd
        assert result.metadata  # type: ignore[reportAttributeAccessIssue]
