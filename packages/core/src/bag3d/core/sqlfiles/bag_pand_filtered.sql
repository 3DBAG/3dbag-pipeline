DROP TABLE IF EXISTS ${new_table} CASCADE;
DROP TABLE IF EXISTS ${bag_pand_removed} CASCADE;

CREATE TABLE ${new_table} AS
SELECT bag.*
FROM ${bag_pand} bag
LEFT JOIN ${bag_bgt_join} bbj ON bag.identificatie = bbj.identificatie
LEFT JOIN ${bag_bag_overlap} bbo ON bag.fid = bbo.fid
WHERE NOT (
    (bbj.identificatie IS NULL AND st_area(bag.geometrie) > 1000)
    OR
    (bbj.identificatie IS NOT NULL
     AND st_area(bbj.bgt_geometrie) / st_area(bbj.bag_geometrie) < 0.1)
    OR
    (bbo.b3_bag_bag_overlap / st_area(bag.geometrie) > 0.1)
);

CREATE TABLE ${bag_pand_removed} AS
SELECT
    bag.identificatie,
    bag.geometrie,
    bbj.bgt_geometrie,
    bag.status,
    ARRAY_REMOVE(
        ARRAY[
            CASE WHEN bbj.relatievehoogteligging < 0
                THEN 'BGT relatieve hoogteligging < 0' END,
            CASE WHEN bbj.identificatie IS NULL
                AND bbo.b3_bag_bag_overlap / st_area(bag.geometrie) > 0.1
                AND cardinality(bbo.b3_bag_bag_overlap_ids) > 1
                THEN 'No BGT match and overlap with more than 1 building' END,
            CASE WHEN bbj.identificatie IS NOT NULL
                AND st_area(bbj.bgt_geometrie) / st_area(bbj.bag_geometrie) < 0.1
                THEN 'BGT-BAG ratio > 0.1' END
        ],
        NULL
    ) AS removal_reasons
FROM ${bag_pand} bag
LEFT JOIN ${bag_bgt_join} bbj ON bag.identificatie = bbj.identificatie
JOIN ${bag_bag_overlap} bbo ON bag.fid = bbo.fid
WHERE
    (bbj.relatievehoogteligging < 0)
    OR
    (bbj.identificatie IS NULL
     AND bbo.b3_bag_bag_overlap / st_area(bag.geometrie) > 0.1
     AND cardinality(bbo.b3_bag_bag_overlap_ids) > 1)
    OR
    (bbj.identificatie IS NOT NULL
     AND st_area(bbj.bgt_geometrie) / st_area(bbj.bag_geometrie) < 0.1);
