DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
SELECT bag.*
FROM ${bag_pand} bag
LEFT JOIN ${bag_bgt_join} bbj ON bag.identificatie = bbj.identificatie
WHERE NOT (
    (bbj.identificatie IS NULL AND st_area(bag.geometrie) > 1000)
    OR
    (bbj.identificatie IS NOT NULL
     AND st_area(bbj.bgt_geometrie) / st_area(bbj.bag_geometrie) < 0.1
     AND st_area(bag.geometrie) > 1000)
    )
;
