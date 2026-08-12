DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
WITH
overground_bgt AS (
    SELECT *
    FROM ${bgt_pand} bt
    WHERE bt.relatievehoogteligging > -1
),
grouped_overground_bgt AS (
    SELECT
        bt.identificatiebagpnd,
        ST_UnaryUnion(ST_Collect(bt.geometrie)) AS geometrie,
        0 AS relatievehoogteligging
    FROM overground_bgt bt
    GROUP BY bt.identificatiebagpnd
),
only_underground_bgt AS (
    SELECT
        bt.identificatiebagpnd,
        bt.geometrie,
        bt.relatievehoogteligging
    FROM ${bgt_pand} bt
    LEFT JOIN grouped_overground_bgt fb
        ON bt.identificatiebagpnd = fb.identificatiebagpnd
    WHERE fb.identificatiebagpnd IS NULL
),
grouped_only_underground_bgt AS (
    SELECT
        obt.identificatiebagpnd,
        ST_UnaryUnion(ST_Collect(obt.geometrie)) AS geometrie,
        obt.relatievehoogteligging
    FROM only_underground_bgt obt
    GROUP BY obt.identificatiebagpnd, obt.relatievehoogteligging
),
grouped_bgt AS (
    SELECT * FROM grouped_overground_bgt
    UNION
    SELECT * FROM grouped_only_underground_bgt
)
SELECT
    bag.identificatie,
    bag.geometrie AS bag_geometrie,
    gbt.geometrie AS bgt_geometrie,
    gbt.relatievehoogteligging
FROM ${bag_pand} bag
JOIN grouped_bgt gbt
    ON gbt.identificatiebagpnd = SUBSTRING(bag.identificatie FROM 15)
    AND bag.geometrie && gbt.geometrie
