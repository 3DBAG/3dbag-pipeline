DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
WITH clusters AS (SELECT identificatie
                       , geometrie
                       , st_clusterintersectingwin(st_buffer(geometrie, 0.1)) OVER () AS cluster
                  FROM ${bag_pand_filtered})
   , counts AS (SELECT *, count(*) OVER (PARTITION BY cluster) AS count_in_cluster
                FROM clusters)
   , woningtype_single AS (SELECT identificatie
                                , geometrie
                                , cluster
                                , CASE
                                      WHEN count_in_cluster = 1
                                          THEN 'vrijstaande woning'
                                      WHEN count_in_cluster = 2 THEN 'twee-onder-een-kap'
                                      WHEN count_in_cluster > 2
                                          THEN 'rijwoning' END AS wt
                           FROM counts)
   , isects AS (SELECT id1 AS identificatie, count(*) AS isect_count
                FROM (SELECT pd1.identificatie AS id1, pd2.identificatie AS id2
                      FROM ${bag_pand_filtered} AS pd1
                               LEFT JOIN ${bag_pand_filtered} AS pd2
                                         ON st_intersects(pd1.geometrie, pd2.geometrie)
                      WHERE pd1.identificatie != pd2.identificatie) AS sub
                GROUP BY id1)
   , wtype_isect AS (SELECT *
                     FROM woningtype_single
                              LEFT JOIN isects USING (identificatie))
SELECT i.identificatie
     , CASE
           WHEN i.wt = 'rijwoning' AND i.isect_count = 1 THEN 'hoekwoning'
           WHEN i.wt = 'rijwoning' AND i.isect_count > 1 THEN 'tussenwoning/geschakeld'
           ELSE i.wt END AS woningtypering
FROM wtype_isect AS i
         INNER JOIN ${pand_vbo_single} AS pv USING (identificatie)
UNION
SELECT DISTINCT identificatie, 'appartement' AS woningtypering
FROM ${pand_vbo_multi}
WHERE identificatie IS NOT NULL;

COMMENT ON TABLE ${new_table} IS 'lvbag.pandactueelbestaand objects where the VBO gebruiksdoel contains woonfunctie are classified into vrijstaande woning, twee-onder-een-kap, hoekwoning, tussenwoning/geschakeld, appartement.';

ALTER TABLE ${new_table}
    ADD PRIMARY KEY (identificatie);
