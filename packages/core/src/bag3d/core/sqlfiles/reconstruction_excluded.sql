DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
SELECT fid
     , identificatie
     , kas_warenhuis
     , geometrie
FROM (SELECT bag.fid
           , bag.identificatie
           , bag.geometrie
           , bkw.kas_warenhuis as b3_kas_warenhuis
      FROM ${bag_cleaned} bag
               LEFT JOIN ${bag_kas_warenhuis} bkw USING (fid)) sub
-- At the moment do not exclude anything, because we don't have the AHN data yet.
LIMIT 0;