CREATE TABLE IF NOT EXISTS ${new_table}
(
    tile_id       text,
    hash          text,
    insert_time   timestamptz,
    pdal_info     jsonb,
    boundary      geometry(Polygon, 28992)
);