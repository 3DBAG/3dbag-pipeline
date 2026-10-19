DROP TABLE IF EXISTS ${new_table};
CREATE TABLE ${new_table} AS
SELECT
    bag.identificatie::text AS identificatie,
    ST_Multi(bag.geometrie)::geometry(MultiPolygon, 28992) AS bag_geometrie,
    ST_Multi(ST_UnaryUnion(ST_Collect(bgt.geometrie)))::geometry(MultiPolygon, 28992)
        AS bgt_geometrie
FROM ${bag_pand} bag
JOIN ${bgt_pand} bgt
    ON bgt.identificatiebagpnd = SUBSTRING(bag.identificatie FROM 15)
    AND bag.geometrie && bgt.geometrie
GROUP BY bag.identificatie, bag.geometrie;
ALTER TABLE ${new_table} ADD PRIMARY KEY (identificatie);
