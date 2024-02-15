-- aggregation function
DROP AGGREGATE IF EXISTS floors_estimation.array_concat_agg(varchar []);
CREATE AGGREGATE floors_estimation.array_concat_agg(varchar []) (SFUNC = array_cat, STYPE = varchar []);

DROP TABLE IF EXISTS verblijfsobject_features;
CREATE TEMP table verblijfsobject_features AS (
  SELECT
    p.pandid,
    COUNT(identificatie) AS no_units,
    SUM(oppervlakte) AS net_area,
	floors_estimation.array_concat_agg(DISTINCT gebruiksdoel) AS uses
  FROM
    (
      SELECT
        unnest(pandref) AS pandid,
        identificatie,
        oppervlakte,
		gebruiksdoel
      FROM
        lvbag.verblijfsobjectactueelbestaand
    ) p
  GROUP BY
    p.pandid
);

-- extract features from cbs, ESRI, and BAG for all buildings
DROP TABLE IF EXISTS ${external_features};
CREATE TABLE ${external_features} AS (
SELECT  p.identificatie
       ,p.geometrie
       ,ST_Buffer(p.geometrie,0.1,'join=mitre')                     AS geometrie_buffer
       ,ST_NPoints(ST_SimplifyPreserveTopology(p.geometrie,0.1))    AS no_vertices
       ,ST_PERIMETER(p.geometrie)                                   AS perimeter
       ,ST_Centroid(p.geometrie)                                    AS centroid
       ,v.no_units
       ,v.net_area
       ,v.uses
	   , NULL::int AS building_function
	   , NULL::int AS no_neighbours_100
	   , NULL::int AS no_adjacent_neighbours
	   , NULL::int AS cbs_percent_multihousehold
	   , NULL::int AS cbs_pop_per_km2
	   , NULL::int AS cbs_dist_to_horeca
	   , NULL::int AS buildingtype
FROM lvbag.pandactueelbestaand p
JOIN verblijfsobject_features v
ON p.identificatie = v.pandid
WHERE p.status LIKE 'Pand in gebruik%' 
AND p.eindgeldigheid IS NULL 
);


-- Add indexes
CREATE index ${external_features_table_name}_geom_idx
ON ${external_features} USING gist(geometrie);

CREATE index ${external_features_table_name}_buffer_idx
ON ${external_features} USING gist(geometrie_buffer);

CREATE index ${external_features_table_name}_centroid_idx
ON ${external_features} USING gist(centroid);


-- 'Residential': 0
-- 'Mixed-residential': 1
-- 'Non-residential (single-function)': 2
-- 'Non-residential (multi-function)': 3
-- 'Other': 4
-- 'Unknown': 5
UPDATE ${external_features}
SET building_function = subquery.building_function
FROM
(
	SELECT  identificatie
	       ,CASE WHEN uses = array['woonfunctie']::varchar[] THEN 0
	             WHEN uses != array['woonfunctie']::varchar[] AND 'woonfunctie' = ANY(uses) THEN 1
	             WHEN 'woonfunctie' != ANY(uses) AND uses != array['overige gebruiksfunctie']::varchar[] AND cardinality(uses) = 1 THEN 2
	             WHEN 'woonfunctie' != ANY(uses) AND uses != array['overige gebruiksfunctie']::varchar[] AND cardinality(uses) > 1 THEN 3
	             WHEN uses = array['overige gebruiksfunctie']::varchar[] THEN 4
	             ELSE 5
				 END AS building_function
	FROM ${external_features}
) AS subquery
WHERE ${external_features}.identificatie = subquery.identificatie; 


UPDATE ${external_features}
SET no_adjacent_neighbours = subquery.no_adjacent
FROM
(
	SELECT  a.identificatie
	       ,COUNT(*) AS no_adjacent
	FROM ${external_features} AS a
	JOIN ${external_features} AS b
	ON ST_INTERSECTS(a.geometrie_buffer, b.geometrie)
	WHERE a.identificatie != b.identificatie
	AND a.building_function != 4
	AND a.building_function != 5
	AND b.building_function != 4
	AND b.building_function != 5
	GROUP BY  a.identificatie
) AS subquery
WHERE ${external_features}.identificatie = subquery.identificatie; 

UPDATE ${external_features}
SET no_neighbours_100 = subquery.no_neighbours
FROM
(
	SELECT  a.identificatie
	       ,COUNT(*) AS no_neighbours
	FROM
	(
		SELECT  *
		FROM ${external_features}
		WHERE building_function != 4
		AND building_function != 5 
	) AS a
	JOIN
	(
		SELECT  *
		FROM ${external_features}
		WHERE building_function != 4
		AND building_function != 5 
	) AS b
	ON ST_DWithin(a.centroid, b.centroid, 100)
	WHERE a.identificatie != b.identificatie
	GROUP BY  a.identificatie
) AS subquery
WHERE ${external_features}.identificatie = subquery.identificatie;

DROP TABLE IF EXISTS cbs_data_per_neighbourhood;
CREATE TEMP TABLE cbs_data_per_neighbourhood AS(
SELECT  ckfdn."WijkenEnBuurten"
       ,cb.wkb_geometry                       AS geometrie
       ,ckfdn."PercentageMeergezinswoning_38" AS cbs_percent_multihousehold_2023
       ,ckfdn."Bevolkingsdichtheid_34"        AS cbs_pop_per_km2_2023
       ,ckfdn2."GIHandelEnHoreca_94"          AS cbs_dist_to_horeca_2021
FROM floors_estimation.cbs_key_figures_districts_neighbourhoods_2023 ckfdn
JOIN floors_estimation.cbs_key_figures_districts_neighbourhoods_2021 ckfdn2
ON ckfdn."WijkenEnBuurten" = ckfdn2."WijkenEnBuurten"
JOIN floors_estimation.cbs_buurten cb
ON cb.bu_code = ckfdn."WijkenEnBuurten"
WHERE ckfdn."SoortRegio_2" = 'Buurt' ); 


UPDATE ${external_features}
SET cbs_percent_multihousehold = cdpn.cbs_percent_multihousehold_2023,
cbs_pop_per_km2 = cdpn.cbs_pop_per_km2_2023,
cbs_dist_to_horeca = cdpn.cbs_dist_to_horeca_2021
FROM cbs_data_per_neighbourhood cdpn
WHERE st_intersects(${external_features}.geometrie, cdpn.geometrie);

-- twee-onder-een-kap : 0
-- vrijstaande woning: 1
-- appartement: 2
-- hoekwoning: 3
-- tussenwoning/geschakeld: 4
-- onbekend: 5
UPDATE ${external_features}
SET buildingtype = 
 				 CASE 
	 				 WHEN ebt.woningtypering  = 'twee-onder-een-kap' THEN 0
		             WHEN ebt.woningtypering = 'vrijstaande woning' THEN 1
		             WHEN ebt.woningtypering = 'appartement' THEN 2
		             WHEN ebt.woningtypering = 'hoekwoning' THEN 3
		             WHEN ebt.woningtypering = 'tussenwoning/geschakeld' THEN 4
		             ELSE 5 
				 END 
FROM floors_estimation.esri_building_type ebt 
WHERE concat('NL.IMBAG.Pand.',ebt.identificatie)  = ${external_features}.identificatie;
