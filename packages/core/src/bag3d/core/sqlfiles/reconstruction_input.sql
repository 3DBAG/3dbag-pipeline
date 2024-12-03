DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
WITH extra_attributes AS (SELECT bag.*
                               , bkw.kas_warenhuis as b3_kas_warenhuis,
                               , bbo.b3_bag_bag_overlap
                          FROM ${bag_cleaned} bag
                                   LEFT JOIN ${bag_kas_warenhuis} bkw USING (fid)
                                   LEFT JOIN ${bag_bag_overlap} bbo USING (fid))
   , duplicates AS (SELECT *
                         , ROW_NUMBER() OVER (PARTITION BY identificatie) rn
                    FROM extra_attributes)
SELECT fid
     , oorspronkelijkbouwjaar
     , identificatie
     , status
     , geconstateerd
     , documentdatum
     , documentnummer
     , voorkomenidentificatie
     , begingeldigheid
     , eindgeldigheid
     , tijdstipregistratie
     , eindregistratie
     , tijdstipinactief
     , tijdstipregistratielv
     , tijdstipeindregistratielv
     , tijdstipinactieflv
     , tijdstipnietbaglv
     , b3_kas_warenhuis
     , b3_bag_bag_overlap
     , geometrie
FROM duplicates
WHERE rn = 1;