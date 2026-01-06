DROP SEQUENCE IF EXISTS bgt_wegdeelactueelbestaand_fid_seq;
CREATE SEQUENCE bgt_wegdeelactueelbestaand_fid_seq;

DROP TABLE IF EXISTS ${new_table} CASCADE;
CREATE TABLE ${new_table} AS
WITH dump AS (SELECT ogc_fid, (st_dump(geometrie2d)).geom AS dumpgeom
              FROM ${wegdeel_tbl})
   , lines AS (SELECT ogc_fid, st_makevalid(st_curvetoline(dumpgeom)) AS geometrie
               FROM dump
               WHERE geometrytype(dumpgeom) = 'CURVEPOLYGON')
   , polys AS (SELECT ogc_fid, st_makevalid(dumpgeom) AS geometrie
               FROM dump
               WHERE geometrytype(dumpgeom) = 'POLYGON')
   , fixed AS (SELECT ogc_fid, st_multi(geometrie)::geometry(MultiPolygon, 28992)
               FROM lines
               WHERE geometrytype(geometrie) = 'POLYGON'
                  OR geometrytype(geometrie) = 'MULTIPOLYGON'
               UNION
               SELECT ogc_fid, st_multi(geometrie)::geometry(MultiPolygon, 28992)
               FROM polys
               WHERE geometrytype(geometrie) = 'POLYGON'
                  OR geometrytype(geometrie) = 'MULTIPOLYGON')
   , withgeom AS (SELECT gml_id
                       , objectbegintijd
                       , objecteindtijd
                       , "identificatie.namespace" AS namespace
                       , "identificatie.lokaalid"  AS locaalid
                       , tijdstipregistratie
                       , eindregistratie
                       , lv_publicatiedatum
                       , bronhouder
                       , inonderzoek
                       , relatievehoogteligging
                       , bgt_status
                       , plus_status
                       , bgt_functie
                       , plus_functie
                       , optalud
                       , bgt_fysiekvoorkomen
                       , plus_fysiekvoorkomen
                  FROM ${wegdeel_tbl} p
                           LEFT JOIN fixed USING (ogc_fid)
                  WHERE p.eindregistratie ISNULL
                    AND p.objecteindtijd ISNULL
                    AND p.bgt_status = 'bestaand')
SELECT NEXTVAL('bgt_wegdeelactueelbestaand_fid_seq') AS fid, withgeom.*
FROM withgeom;

DROP SEQUENCE IF EXISTS bgt_wegdeelactueelbestaand_fid_seq;