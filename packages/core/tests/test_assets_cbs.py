"""Tests for CBS download and load assets."""

import csv
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context

from bag3d.common.types import PostgresTableIdentifier
from bag3d.core.assets.cbs.download import (
    CbsBuurtkaartConfig,
    CbsKeyFiguresConfig,
    _clean_cell,
    _fetch_cbs_odata,
    _records_to_csv,
    extract_cbs_buurtkaart,
    extract_cbs_key_figures,
)
from bag3d.core.assets.cbs.load import (
    _load_csv_to_postgres,
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
            ("None", ""),
            ("---", ""),
            ("___", ""),
            ("", ""),
            (42, "42"),
            (3.14, "3.14"),
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
                "ID": "0",
                "Region": "NL",
                "Type": "Land",
                "Code": "NL00",
                "Population": "100",
                "Density": "---",
            },
        ]
        output = tmp_path / "test.csv"
        _records_to_csv(records, output)

        with open(output) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["ID"] == "0"
        assert rows[0]["Population"] == "100"
        assert rows[0]["Density"] == ""

    def test_raises_on_empty_records(self, tmp_path):
        with pytest.raises(ValueError, match="No records"):
            _records_to_csv([], tmp_path / "empty.csv")


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
    def test_saves_csv_per_year(self, tmp_path, monkeypatch):
        cbs_dir = tmp_path / "cbs"
        cbs_dir.mkdir(parents=True)
        file_store = MagicMock()
        file_store.create_subdir.return_value = cbs_dir

        mock_records = [
            {"ID": "0", "Region": "NL", "Type": "Buurt", "Code": "BU00", "Pop": "100"}
        ]
        monkeypatch.setattr(
            "bag3d.core.assets.cbs.download._fetch_cbs_odata",
            lambda url: mock_records,
        )

        config = CbsKeyFiguresConfig(table_ids={"2025": "86165NED"})
        with build_asset_context() as context:
            result = extract_cbs_key_figures(context, config, file_store)

        csv_path = cbs_dir / "cbs_key_figures_2025.csv"
        assert result.metadata["File [2025]"].value == str(csv_path)
        assert result.metadata["Records [2025]"].value == 1
        assert csv_path.exists()


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

        assert result.metadata["GeoPackage"].value == str(fallback)

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
# _load_csv_to_postgres
# ---------------------------------------------------------------------------
class TestLoadCsvToPostgres:
    def test_creates_table_with_text_columns(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "test.csv"
        csv_path.write_text('"ID","Name"\n"1","test"\n')

        mock_conn = MagicMock()
        mock_conn.dsn = "host=localhost dbname=test"
        mock_db = MagicMock()
        mock_db.connection = mock_conn

        # The COPY step requires a real psycopg connection — patch the
        # connect call to raise after the CREATE TABLE is verified instead.
        original = _load_csv_to_postgres

        def patched_load_csv(computation_db, csv_path, table):
            original(computation_db, csv_path, table)

        monkeypatch.setattr(
            "bag3d.core.assets.cbs.load.connect",
            lambda dsn: (_ for _ in ()).throw(RuntimeError("stop after CREATE TABLE")),
        )

        table = PostgresTableIdentifier("cbs", "test_table")
        try:
            _load_csv_to_postgres(mock_db, csv_path, table)
        except RuntimeError:
            pass

        create_call = mock_conn.send_query.call_args_list[0][0][0]
        sql_str = str(create_call)
        assert '"ID" TEXT' in sql_str
        assert '"Name" TEXT' in sql_str


# ---------------------------------------------------------------------------
# cbs_key_figures
# ---------------------------------------------------------------------------
class TestCbsKeyFigures:
    def test_adds_primary_key_and_comment(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "test.csv"
        csv_path.write_text('"ID","Region"\n"1","NL"\n')
        mock_conn = MagicMock()
        mock_conn.dsn = "host=localhost dbname=test"
        mock_db = MagicMock()
        mock_db.connection = mock_conn

        monkeypatch.setattr(
            "bag3d.core.assets.cbs.load.connect",
            lambda dsn: (_ for _ in ()).throw(RuntimeError("stop after COPY")),
        )

        with build_asset_context() as context:
            try:
                cbs_key_figures(context, mock_db, {"2025": csv_path})
            except RuntimeError:
                pass

        # CREATE TABLE and DROP TABLE should have been sent before COPY
        calls = [str(c[0][0]) for c in mock_conn.send_query.call_args_list]
        sql_str = " ".join(calls)
        assert "key_figures_districts_neighbourhoods" in sql_str
        assert '"ID" TEXT' in sql_str


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
        assert result.metadata
