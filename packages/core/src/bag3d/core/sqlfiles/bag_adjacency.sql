DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
WITH pairs
    AS MATERIALIZED (SELECT pd1.identificatie AS id1
                          , pd2.identificatie AS id2
                     FROM ${bag_pand} AS pd1
                              JOIN ${bag_pand} AS pd2
                                   ON pd1.identificatie < pd2.identificatie
                                       AND pd1.geometrie &&
                                           ST_Expand(pd2.geometrie, 0.1)
                              CROSS JOIN LATERAL ( SELECT ST_Multi(ST_CollectionExtract(
                             ST_MakeValid(pd1.geometrie), 3))     AS g1
                                                        , ST_Multi(ST_CollectionExtract(
                                 ST_MakeValid(pd2.geometrie),
                                 3))                              AS g2
                         ) AS norm
                     WHERE NOT ST_IsEmpty(norm.g1)
                       AND NOT ST_IsEmpty(norm.g2)
                       AND ST_DWithin(norm.g1, norm.g2, 0.1)
                       AND ST_Length(ST_CollectionExtract(
                             ST_Intersection(ST_Boundary(norm.g1),
                                             ST_Boundary(norm.g2)), 2)) >= 0.5
    )
   , lower_neighbors
    AS (SELECT id2                         AS identificatie
             , array_agg(id1 ORDER BY id1) AS ids
        FROM pairs
        GROUP BY id2)
   , upper_neighbors
    AS (SELECT id1                         AS identificatie
             , array_agg(id2 ORDER BY id2) AS ids
        FROM pairs
        GROUP BY id1)

SELECT pd.identificatie
     , array_cat(COALESCE(ln.ids, '{{}}'::text[]),
                 COALESCE(un.ids, '{{}}'::text[])) AS adjacent_ids
FROM ${bag_pand} AS pd
         LEFT JOIN lower_neighbors AS ln ON ln.identificatie = pd.identificatie
         LEFT JOIN upper_neighbors AS un ON un.identificatie = pd.identificatie;
