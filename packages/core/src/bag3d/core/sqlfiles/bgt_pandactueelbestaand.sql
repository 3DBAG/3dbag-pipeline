DROP SEQUENCE IF EXISTS bgt_pandactueelbestaand_fid_seq;
CREATE SEQUENCE bgt_pandactueelbestaand_fid_seq;

DROP TABLE IF EXISTS ${new_table} CASCADE;
CREATE TABLE ${new_table} AS
SELECT NEXTVAL('bgt_pandactueelbestaand_fid_seq') AS fid
     , gml_id
     , objectbegintijd
     , objecteindtijd
     , "identificatie.namespace" AS namespace
     , "identificatie.lokaalid"  AS lokaalid
     , tijdstipregistratie
     , eindregistratie
     , lv_publicatiedatum
     , bronhouder
     , inonderzoek
     , relatievehoogteligging
     , bgt_status
     , plus_status
     , identificatiebagpnd
     , CASE 
         WHEN geometrytype(geometrie2d) = 'CURVEPOLYGON' THEN
             st_multi(st_makevalid(st_curvetoline(geometrie2d)))::geometry(MultiPolygon, 28992)
         WHEN geometrytype(geometrie2d) = 'MULTICURVEPOLYGON' THEN
             st_multi(st_makevalid(st_curvetoline(geometrie2d)))::geometry(MultiPolygon, 28992)
         WHEN geometrytype(geometrie2d) = 'MULTISURFACE' THEN
             st_multi(st_makevalid(st_collectionextract(st_curvetoline(geometrie2d), 3)))::geometry(MultiPolygon, 28992)
         ELSE
             st_multi(st_makevalid(geometrie2d))::geometry(MultiPolygon, 28992)
       END as geometrie
FROM ${pand_tbl}
WHERE eindregistratie IS NULL
  AND objecteindtijd IS NULL
  AND bgt_status = 'bestaand';

DROP SEQUENCE IF EXISTS bgt_pandactueelbestaand_fid_seq;