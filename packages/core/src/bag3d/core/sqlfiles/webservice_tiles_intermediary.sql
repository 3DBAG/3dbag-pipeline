CREATE TABLE ${export_index}
(
    tile_id      text,
    has_cityjson boolean,
    has_gpkg boolean,
    has_obj  boolean,
    wkt          geometry(Polygon, 28992)
);

CREATE TABLE ${validate_compressed_files}
(
    tile_id            text,
    cj_zip_ok          text,
    cj_nr_features     integer,
    cj_nr_invalid      integer,
    cj_all_errors      text,
    cj_schema_valid    text,
    cj_schema_warnings text,
    cj_lod             text,
    cj_download        text,
    cj_sha256          text,
    obj_zip_ok         text,
    obj_nr_features    integer,
    obj_nr_invalid     text,
    obj_all_errors     text,
    obj_download       text,
    obj_sha256         text,
    gpkg_zip_ok        text,
    gpkg_ok            text,
    gpkg_nr_features   integer,
    gpkg_download      text,
    gpkg_sha256        text
);



