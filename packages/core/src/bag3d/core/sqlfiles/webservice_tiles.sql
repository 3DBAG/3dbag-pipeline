-- WARNING: geoserver does not understand postgres arrays

-- The 'tiles' layer for the webservices
CREATE TABLE ${new_table} AS
WITH validate_compressed_files_cast AS (SELECT tile_id::text
                                             , CASE
                                                   WHEN cj_zip_ok ISNULL OR length(cj_zip_ok) = 0
                                                       THEN NULL::boolean
                                                   ELSE cj_zip_ok::boolean END                     AS cj_zip_ok
                                             , CASE
                                                   WHEN cj_nr_building ISNULL OR length(cj_nr_building) = 0
                                                       THEN NULL::int
                                                   ELSE cj_nr_building::int END                    AS cj_nr_building
                                             , CASE
                                                   WHEN cj_nr_buildingpart ISNULL OR
                                                        length(cj_nr_buildingpart) = 0
                                                       THEN NULL::int
                                                   ELSE cj_nr_buildingpart::int END                AS cj_nr_buildingpart
                                             , CASE
                                                   WHEN cj_nr_invalid_building ISNULL OR
                                                        length(cj_nr_invalid_building) =
                                                        0 THEN NULL::int
                                                   ELSE cj_nr_invalid_building::int END            AS cj_nr_invalid_building
                                             , CASE
                                                   WHEN
                                                       cj_nr_invalid_buildingpart_lod12 ISNULL OR
                                                       length(cj_nr_invalid_buildingpart_lod12) =
                                                       0 THEN NULL::int
                                                   ELSE cj_nr_invalid_buildingpart_lod12::int END  AS cj_nr_invalid_buildingpart_lod12
                                             , CASE
                                                   WHEN
                                                       cj_nr_invalid_buildingpart_lod13 ISNULL OR
                                                       length(cj_nr_invalid_buildingpart_lod13) =
                                                       0 THEN NULL::int
                                                   ELSE cj_nr_invalid_buildingpart_lod13::int END  AS cj_nr_invalid_buildingpart_lod13
                                             , CASE
                                                   WHEN
                                                       cj_nr_invalid_buildingpart_lod22 ISNULL OR
                                                       length(cj_nr_invalid_buildingpart_lod22) =
                                                       0 THEN NULL::int
                                                   ELSE cj_nr_invalid_buildingpart_lod22::int END  AS cj_nr_invalid_buildingpart_lod22
                                             , CASE
                                                   WHEN cj_errors_lod12 ISNULL OR
                                                        length(cj_errors_lod12) = 0
                                                       THEN NULL::int[]
                                                   ELSE string_to_array(replace(
                                                                                replace(cj_errors_lod12, '[', ''),
                                                                                ']',
                                                                                ''),
                                                                        ',')::int[] END            AS cj_errors_lod12
                                             , CASE
                                                   WHEN cj_errors_lod13 ISNULL OR
                                                        length(cj_errors_lod13) = 0
                                                       THEN NULL::int[]
                                                   ELSE string_to_array(replace(
                                                                                replace(cj_errors_lod13, '[', ''),
                                                                                ']',
                                                                                ''),
                                                                        ',')::int[] END            AS cj_errors_lod13
                                             , CASE
                                                   WHEN cj_errors_lod22 ISNULL OR
                                                        length(cj_errors_lod22) = 0
                                                       THEN NULL::int[]
                                                   ELSE string_to_array(replace(
                                                                                replace(cj_errors_lod22, '[', ''),
                                                                                ']',
                                                                                ''),
                                                                        ',')::int[] END            AS cj_errors_lod22
                                             , CASE
                                                   WHEN
                                                       cj_nr_mismatch_errors_lod12 ISNULL OR
                                                       length(cj_nr_mismatch_errors_lod12) =
                                                       0 THEN NULL::int
                                                   ELSE cj_nr_mismatch_errors_lod12::int END       AS cj_nr_mismatch_errors_lod12
                                             , CASE
                                                   WHEN
                                                       cj_nr_mismatch_errors_lod13 ISNULL OR
                                                       length(cj_nr_mismatch_errors_lod13) =
                                                       0 THEN NULL::int
                                                   ELSE cj_nr_mismatch_errors_lod13::int END       AS cj_nr_mismatch_errors_lod13
                                             , CASE
                                                   WHEN
                                                       cj_nr_mismatch_errors_lod22 ISNULL OR
                                                       length(cj_nr_mismatch_errors_lod22) =
                                                       0 THEN NULL::int
                                                   ELSE cj_nr_mismatch_errors_lod22::int END       AS cj_nr_mismatch_errors_lod22
                                             , CASE
                                                   WHEN cj_schema_valid ISNULL OR
                                                        length(cj_schema_valid) = 0
                                                       THEN NULL::boolean
                                                   ELSE cj_schema_valid::boolean END               AS cj_schema_valid
                                             , CASE
                                                   WHEN cj_schema_warnings ISNULL OR
                                                        length(cj_schema_warnings) = 0
                                                       THEN NULL::boolean
                                                   ELSE cj_schema_warnings::boolean END            AS cj_schema_warnings
                                             , CASE
                                                   WHEN cj_lod ISNULL OR length(cj_lod) = 0
                                                       THEN NULL::text[]
                                                   ELSE string_to_array(
                                                           replace(replace(cj_lod, '[', ''), ']', ''),
                                                           ',')::text[] END                        AS cj_lod
                                             , cj_download::text
                                             , cj_sha256::text
                                             , CASE
                                                   WHEN obj_zip_ok ISNULL OR length(obj_zip_ok) = 0
                                                       THEN NULL::boolean
                                                   ELSE obj_zip_ok::boolean END                    AS obj_zip_ok
                                             , CASE
                                                   WHEN obj_nr_building ISNULL OR
                                                        length(obj_nr_building) = 0
                                                       THEN NULL::int
                                                   ELSE obj_nr_building::int END                   AS obj_nr_building
                                             , CASE
                                                   WHEN obj_nr_buildingpart ISNULL OR
                                                        length(obj_nr_buildingpart) = 0
                                                       THEN NULL::int
                                                   ELSE obj_nr_buildingpart::int END               AS obj_nr_buildingpart
                                             , CASE
                                                   WHEN
                                                       obj_nr_invalid_building ISNULL OR
                                                       length(obj_nr_invalid_building) =
                                                       0 THEN NULL::int
                                                   ELSE obj_nr_invalid_building::int END           AS obj_nr_invalid_building
                                             , CASE
                                                   WHEN
                                                       obj_nr_invalid_buildingpart_lod12 ISNULL OR
                                                       length(obj_nr_invalid_buildingpart_lod12) =
                                                       0 THEN NULL::int
                                                   ELSE obj_nr_invalid_buildingpart_lod12::int END AS obj_nr_invalid_buildingpart_lod12
                                             , CASE
                                                   WHEN
                                                       obj_nr_invalid_buildingpart_lod13 ISNULL OR
                                                       length(obj_nr_invalid_buildingpart_lod13) =
                                                       0 THEN NULL::int
                                                   ELSE obj_nr_invalid_buildingpart_lod13::int END AS obj_nr_invalid_buildingpart_lod13
                                             , CASE
                                                   WHEN
                                                       obj_nr_invalid_buildingpart_lod22 ISNULL OR
                                                       length(obj_nr_invalid_buildingpart_lod22) =
                                                       0 THEN NULL::int
                                                   ELSE obj_nr_invalid_buildingpart_lod22::int END AS obj_nr_invalid_buildingpart_lod22
                                             , CASE
                                                   WHEN obj_errors_lod12 ISNULL OR
                                                        length(obj_errors_lod12) = 0
                                                       THEN NULL::int[]
                                                   ELSE string_to_array(replace(
                                                                                replace(obj_errors_lod12, '[', ''),
                                                                                ']',
                                                                                ''),
                                                                        ',')::int[] END            AS obj_errors_lod12
                                             , CASE
                                                   WHEN obj_errors_lod13 ISNULL OR
                                                        length(obj_errors_lod13) = 0
                                                       THEN NULL::int[]
                                                   ELSE string_to_array(replace(
                                                                                replace(obj_errors_lod13, '[', ''),
                                                                                ']',
                                                                                ''),
                                                                        ',')::int[] END            AS obj_errors_lod13
                                             , CASE
                                                   WHEN obj_errors_lod22 ISNULL OR
                                                        length(obj_errors_lod22) = 0
                                                       THEN NULL::int[]
                                                   ELSE string_to_array(replace(
                                                                                replace(obj_errors_lod22, '[', ''),
                                                                                ']',
                                                                                ''),
                                                                        ',')::int[] END            AS obj_errors_lod22
                                             , obj_download::text
                                             , obj_sha256::text
                                             , CASE
                                                   WHEN gpkg_zip_ok ISNULL OR length(gpkg_zip_ok) = 0
                                                       THEN NULL::boolean
                                                   ELSE gpkg_zip_ok::boolean END                   AS gpkg_zip_ok
                                             , CASE
                                                   WHEN gpkg_file_ok ISNULL OR length(gpkg_file_ok) = 0
                                                       THEN NULL::boolean
                                                   ELSE gpkg_file_ok::boolean END                  AS gpkg_file_ok
                                             , CASE
                                                   WHEN gpkg_nr_building ISNULL OR
                                                        length(gpkg_nr_building) = 0
                                                       THEN NULL::int
                                                   ELSE gpkg_nr_building::int END                  AS gpkg_nr_building
                                             , CASE
                                                   WHEN gpkg_nr_buildingpart ISNULL OR
                                                        length(gpkg_nr_buildingpart) = 0
                                                       THEN NULL::int
                                                   ELSE gpkg_nr_buildingpart::int END              AS gpkg_nr_buildingpart
                                             , gpkg_download::text
                                             , gpkg_sha256::text
                                        FROM ${validate_compressed_files})
SELECT af.*, ti.wkt AS geometry
FROM ${export_index} ti
         JOIN validate_compressed_files_cast af USING (tile_id);
