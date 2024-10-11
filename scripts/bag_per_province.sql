/*
Download the latest administrative units GPKG from https://www.nationaalgeoregister.nl/geonetwork/srv/api/records/208bc283-7c66-4ce7-8ad3-1cf3e8933fb5?language=all
E.g.: https://service.pdok.nl/kadaster/bestuurlijkegebieden/atom/v1_0/downloads/BestuurlijkeGebieden_2024.gpkg
Load the tables into adminitrative_units, appending the year to the table names.
E.g.: ogr2ogr -f PostgreSQL -nln administrative_units.provinciegebied_2024 PG:"dbname=baseregisters host=localhost port=5432 user=etl active_schema=administrative_unis" BestuurlijkeGebieden_2024.gpkg provinciegebied
*/
CREATE VIEW lvbag.pandactueelbestaand_provinces AS
SELECT bag.*, p.naam AS provincienaam
FROM lvbag.pandactueelbestaand AS bag
         JOIN administrative_units.provinciegebied_2024 AS p
              ON st_contains(p.geom, st_centroid(bag.geometrie));
COMMENT ON VIEW lvbag.pandactueelbestaand_provinces IS 'Each polygon in lvbag.pandactueelbestand assigned to a province (2024).';
