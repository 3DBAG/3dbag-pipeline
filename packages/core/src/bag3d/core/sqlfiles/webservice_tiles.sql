-- WARNING: geoserver does not understand postgres arrays

-- The 'tiles' layer for the webservices
CREATE TABLE ${new_table} AS
WITH validate_compressed_files_cast AS (SELECT tile_id::text
                                             , CASE
                                                   WHEN cj_zip_ok ISNULL OR length(cj_zip_ok) = 0
                                                       THEN NULL::boolean
                                                   ELSE cj_zip_ok::boolean END                                   AS cj_zip_ok
                                             , cj_nr_features::int
                                             , cj_nr_invalid::int
                                             , CASE
                                                   WHEN cj_all_errors ISNULL OR length(cj_all_errors) = 0
                                                       THEN NULL::int[]
                                                   ELSE replace(
                                                           replace(cj_all_errors, '[', '{'),
                                                           ']',
                                                           '}')::int[] END                                       AS cj_all_errors
                                             , CASE
                                                   WHEN cj_schema_valid ISNULL OR
                                                        length(cj_schema_valid) = 0
                                                       THEN NULL::boolean
                                                   ELSE cj_schema_valid::boolean END                             AS cj_schema_valid
                                             , CASE
                                                   WHEN cj_schema_warnings ISNULL OR
                                                        length(cj_schema_warnings) = 0
                                                       THEN NULL::boolean
                                                   ELSE cj_schema_warnings::boolean END                          AS cj_schema_warnings
                                             , CASE
                                                   WHEN cj_lod ISNULL OR length(cj_lod) = 0
                                                       THEN NULL::text[]
                                                   ELSE replace(replace(cj_lod, '[', '{'), ']', '}')::text[] END AS cj_lod
                                             , cj_download::text
                                             , cj_sha256::text
                                             , CASE
                                                   WHEN obj_zip_ok ISNULL OR length(obj_zip_ok) = 0
                                                       THEN NULL::boolean
                                                   ELSE obj_zip_ok::boolean END                                  AS obj_zip_ok
                                             , obj_nr_features::int
                                             , CASE
                                                   WHEN obj_nr_invalid ISNULL OR length(obj_nr_invalid) = 0
                                                       THEN NULL::int[]
                                                   ELSE replace(
                                                           replace(obj_nr_invalid, '[', '{'),
                                                           ']',
                                                           '}')::int[] END                                       AS obj_nr_invalid
                                             , CASE
                                                   WHEN obj_all_errors ISNULL OR length(obj_all_errors) = 0
                                                       THEN NULL::int[]
                                                   ELSE replace(
                                                           replace(obj_all_errors, '[', '{'),
                                                           ']',
                                                           '}')::int[] END                                       AS obj_all_errors
                                             , obj_download::text
                                             , obj_sha256::text
                                             , CASE
                                                   WHEN gpkg_zip_ok ISNULL OR length(gpkg_zip_ok) = 0
                                                       THEN NULL::boolean
                                                   ELSE gpkg_zip_ok::boolean END                                 AS gpkg_zip_ok
                                             , CASE
                                                   WHEN gpkg_ok ISNULL OR length(gpkg_ok) = 0
                                                       THEN NULL::boolean
                                                   ELSE gpkg_ok::boolean END                                     AS gpkg_ok
                                             , gpkg_nr_features::int
                                             , gpkg_download::text
                                             , gpkg_sha256::text
                                        FROM ${validate_compressed_files})
SELECT ti.tile_id
     , af.cj_zip_ok
     , af.cj_nr_features
     , af.cj_nr_invalid
     , af.cj_all_errors
     , af.cj_schema_valid
     , af.cj_schema_warnings
     , af.cj_lod
     , af.cj_download
     , af.cj_sha256
     , af.obj_zip_ok
     , af.obj_nr_features
     , af.obj_nr_invalid
     , af.obj_all_errors
     , af.obj_download
     , af.obj_sha256
     , af.gpkg_zip_ok
     , af.gpkg_ok
     , af.gpkg_nr_features
     , af.gpkg_download
     , af.gpkg_sha256
     , ti.wkt     AS geometry
FROM ${export_index} ti
         JOIN validate_compressed_files_cast af USING (tile_id);
