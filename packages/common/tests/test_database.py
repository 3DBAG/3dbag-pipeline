from unittest.mock import Mock

import pytest
from pgutils import PostgresConnection, PostgresTableIdentifier

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.utils.database import postgrestable_metadata


@pytest.mark.parametrize("empty", [False, True])
def test_table_metadata_previews_json_values_without_slicing_them(empty):
    fields = [
        ("metadata", "jsonb"),
        ("candidates", "ARRAY"),
        ("height", "double precision"),
        ("missing", "text"),
        ("description", "text"),
        ("geometry", "geometry"),
    ]
    records = (
        []
        if empty
        else [
            {
                "metadata": {"status": "no_laz_tiles", "points": 0},
                "candidates": [2.5, 3.0],
                "height": 2.5,
                "missing": None,
                "description": "a|b\r\nc",
                "geometry": "0123456789" * 100,
            }
        ]
    )
    connection = Mock(spec=PostgresConnection, dbname="test")
    connection.get_count.return_value = len(records)
    connection.get_fields.return_value = fields
    connection.count_nulls.return_value = [
        {"column_name": name, "missing_values": 0} for name, _ in fields
    ]
    connection.get_dict.return_value = records
    # Exercise the installed pgutils implementation, including the buggy
    # Markdown path that this regression must avoid.
    connection.get_head.side_effect = lambda *args, **kwargs: (
        PostgresConnection.get_head(connection, *args, **kwargs)
    )
    database = Mock(spec=DatabaseResource, connection=connection)

    metadata = postgrestable_metadata(
        database, PostgresTableIdentifier("underpasses", "heights")
    )

    assert metadata["Rows"] == len(records)
    assert metadata["Database.Schema.Table"] == "test.underpasses.heights"
    head = metadata["Head"].value
    assert head.splitlines()[0] == (
        "| metadata | candidates | height | missing | description | geometry |"
    )
    assert len(head.splitlines()) == (2 if empty else 3)
    if not empty:
        assert str(records[0]["metadata"])[:24] in head
        assert "[2.5, 3.0] | 2.5 | None | a\\|b  c" in head
        assert "012345678901234567890123" in head
        assert "0123456789012345678901234" not in head
