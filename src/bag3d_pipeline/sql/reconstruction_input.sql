DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
WITH bag_kas AS (SELECT bag.*
                      , bkw.kas_warenhuis
                 FROM ${bag_cleaned} bag
                          LEFT JOIN ${bag_kas_warenhuis} bkw USING (fid))
   , ahn AS (SELECT bag.fid,
                    (ahn.pdal_info - > 'metadata' - > 'creation_year') ::int4 AS ahn_jaar
             FROM ${bag_cleaned} bag
                      JOIN ${metadata_ahn} ahn
                           ON st_intersects(bag.geometrie, ahn.boundary))
   , ahn_max_year_per_building AS (SELECT fid, MAX(ahn_jaar) AS ahn_jaar
                                   FROM ahn
                                   GROUP BY fid)
   , bag_kas_ahn AS (SELECT bag_kas.*, a.ahn_jaar
                     FROM bag_kas
                              JOIN ahn_max_year_per_building a USING (fid))
   , duplicates AS (SELECT *
                         , ROW_NUMBER() OVER (PARTITION BY identificatie) rn
                    FROM bag_kas_ahn)
SELECT fid,
       oorspronkelijkbouwjaar,
       identificatie,
       status,
       geconstateerd,
       documentdatum,
       documentnummer,
       voorkomenidentificatie,
       begingeldigheid,
       eindgeldigheid,
       tijdstipregistratie,
       eindregistratie,
       tijdstipinactief,
       tijdstipregistratielv,
       tijdstipeindregistratielv,
       tijdstipinactieflv,
       tijdstipnietbaglv,
       kas_warenhuis,
       geometrie,
FROM duplicates
WHERE rn = 1
  AND oorspronkelijkbouwjaar < ahn_jaar;