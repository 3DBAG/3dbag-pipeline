DROP TABLE IF EXISTS ${new_table} CASCADE;

WITH isect AS (SELECT bag1.fid
                    , ST_Intersection(bag1.geometrie, bag2.geometrie) isection
               FROM ${bag_cleaned} bag1
                        JOIN
                    ${bag_cleaned} bag2
                    ON ST_Intersects(bag1.geometrie, bag2.geometrie)
               WHERE bag1.fid != bag2.fid)
   , dissolve AS (SELECT fid
                       , st_area(st_unaryunion(st_collect(isection))) overlap_area
                  FROM isect
                  GROUP BY fid)
SELECT d.fid
     , CASE
           WHEN overlap_area IS NULL OR overlap_area < 1.0
               THEN 0.0
           ELSE overlap_area
    END AS b3_bag_bag_overlap
--      , CASE
--            WHEN overlap_area IS NULL OR overlap_area < 1.0
--                THEN 0.0
--            ELSE (overlap_area / st_area(b.geometrie)) * 100.0
--     END AS b3_bag_bag_overlap_pct
FROM dissolve d
         JOIN ${bag_cleaned} b ON d.fid = b.fid;