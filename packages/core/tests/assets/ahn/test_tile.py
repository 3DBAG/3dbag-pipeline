from bag3d.core.assets.ahn.tile import  regular_grid_200m
from pgutils import PostgresTableIdentifier
from bag3d.common.utils.database import drop_table, table_exists


def test_regular_grid_200m(context):
    res = regular_grid_200m(context)
    assert isinstance(res.value, PostgresTableIdentifier)
    new_table = PostgresTableIdentifier('ahn', "regular_grid_200m")
    assert  str(res.value) == f'{new_table.schema}.{new_table.table}'
    assert table_exists(context, new_table) is True
  