DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
WITH isect AS (SELECT bag.fid
                    , st_area(st_intersection(bag.geometrie,
                                              top.geometrie_vlak)) intersection_area_part
               FROM ${bag_cleaned} bag
                  , ${top10nl_gebouw} top
               WHERE top.typegebouw && ARRAY ['kas, warenhuis']
                 AND st_intersects(bag.geometrie, top.geometrie_vlak))
   , sum_area AS (SELECT fid
                       , SUM(intersection_area_part) isect_area
                  FROM isect
                  GROUP BY fid)
   , kases AS (SELECT s.fid
                    , CASE
                          WHEN s.isect_area / st_area(t.geometrie) > 0.11 THEN TRUE
                          ELSE FALSE
        END AS kas_warenhuis
               FROM sum_area s
                        INNER JOIN ${bag_cleaned} t USING (fid))
SELECT t.fid
     , CASE
           WHEN kwt.fid IS NULL THEN FALSE
           ELSE kwt.kas_warenhuis
    END AS kas_warenhuis
FROM ${bag_cleaned} t
         LEFT JOIN kases kwt USING (fid);