DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
SELECT DISTINCT p1.identificatie, p2.identificatie AS adjacent_identificatie
FROM ${bag_pand_filtered} AS p1
         JOIN ${bag_pand_filtered} AS p2 ON p1.identificatie <> p2.identificatie AND p1.geometrie && ST_Expand(p2.geometrie, 0.1)
   AND ST_DWithin(p1.geometrie, p2.geometrie, 0.1);
