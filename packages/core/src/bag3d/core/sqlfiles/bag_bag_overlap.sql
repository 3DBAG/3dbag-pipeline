DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
WITH isect AS (
    SELECT bag1.fid
         , ST_Intersection(bag1.geometrie, bag2.geometrie) isection
         , bag2.identificatie AS overlapping_identificatie
    FROM ${bag_pandactueelbestaand} bag1
    JOIN ${bag_pandactueelbestaand} bag2
        ON ST_Intersects(bag1.geometrie, bag2.geometrie)
    WHERE bag1.fid != bag2.fid
)
, dissolve AS (
    SELECT fid
         , st_area(st_unaryunion(st_collect(isection))) overlap_area
         , ARRAY_AGG(DISTINCT overlapping_identificatie ORDER BY overlapping_identificatie) AS overlapping_ids
    FROM isect
    GROUP BY fid
)
SELECT b.fid
     , COALESCE(
           CASE WHEN d.overlap_area < 1.0 THEN 0.0 ELSE d.overlap_area END,
           0.0
       ) AS b3_bag_bag_overlap
     , COALESCE(d.overlapping_ids, ARRAY[]::text[]) AS b3_bag_bag_overlap_ids
FROM ${bag_pandactueelbestaand} b
LEFT JOIN dissolve d ON b.fid = d.fid;
