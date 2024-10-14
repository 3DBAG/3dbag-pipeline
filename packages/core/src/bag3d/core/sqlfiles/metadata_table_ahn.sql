CREATE TABLE IF NOT EXISTS ${new_table}
(
    tile_id       text PRIMARY KEY,
    hash          text,
    download_time timestamptz,
    pdal_info     jsonb,
    boundary      geometry(Polygon, 28992)
);