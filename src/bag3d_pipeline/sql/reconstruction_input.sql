DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
WITH bag_kas AS (SELECT bag.*
                      , bkw.kas_warenhuis
                 FROM ${bag_cleaned} bag
                          LEFT JOIN ${bag_kas_warenhuis} bkw USING (fid))
   , duplicates AS (SELECT *
                         , ROW_NUMBER() OVER (PARTITION BY identificatie) rn
                    FROM bag_kas)
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
     , kas_warenhuis
     , geometrie
FROM duplicates
WHERE rn = 1;