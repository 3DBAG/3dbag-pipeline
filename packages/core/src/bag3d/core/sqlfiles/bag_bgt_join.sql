DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
WITH filtered AS (
    SELECT
        bag.identificatie,
        bag.geometrie AS bag_geometrie,
        bt.geometrie AS bgt_geometrie
    FROM ${bag_pand} bag
    JOIN ${bgt_pand} bt
        ON bt.identificatiebagpnd = SUBSTRING(bag.identificatie FROM 15)
        AND bag.geometrie && bt.geometrie
)
SELECT
    identificatie,
    bag_geometrie,
    ST_UnaryUnion(ST_Collect(bgt_geometrie)) AS bgt_geometrie
FROM filtered
GROUP BY identificatie, bag_geometrie;
