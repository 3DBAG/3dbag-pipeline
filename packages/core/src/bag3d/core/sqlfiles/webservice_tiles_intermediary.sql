CREATE TABLE ${export_index}
(
    tile_id      text,
    has_cityjson boolean,
    has_gpkg     boolean,
    has_obj      boolean,
    wkt          geometry(Polygon, 28992)
);

CREATE TABLE ${validate_compressed_files}
(
    tile_id                           text,
    cj_zip_ok                         text,
    cj_nr_building                    text,
    cj_nr_buildingpart                text,
    cj_nr_invalid_building            text,
    cj_nr_invalid_buildingpart_lod12  text,
    cj_nr_invalid_buildingpart_lod13  text,
    cj_nr_invalid_buildingpart_lod22  text,
    cj_errors_lod12                   text,
    cj_errors_lod13                   text,
    cj_errors_lod22                   text,
    cj_nr_mismatch_errors_lod12       text,
    cj_nr_mismatch_errors_lod13       text,
    cj_nr_mismatch_errors_lod22       text,
    cj_lod                            text,
    cj_schema_valid                   text,
    cj_schema_warnings                text,
    cj_download                       text,
    cj_sha256                         text,
    obj_zip_ok                        text,
    obj_nr_building                   text,
    obj_nr_buildingpart               text,
    obj_nr_invalid_building           text,
    obj_nr_invalid_buildingpart_lod12 text,
    obj_nr_invalid_buildingpart_lod13 text,
    obj_nr_invalid_buildingpart_lod22 text,
    obj_errors_lod12                  text,
    obj_errors_lod13                  text,
    obj_errors_lod22                  text,
    obj_download                      text,
    obj_sha256                        text,
    gpkg_zip_ok                       text,
    gpkg_file_ok                      text,
    gpkg_nr_building                  text,
    gpkg_nr_buildingpart              text,
    gpkg_download                     text,
    gpkg_sha256                       text
);
