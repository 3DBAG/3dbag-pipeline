DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
SELECT ogc_fid                                            AS fid
     , namespace::varchar(10)
     , lokaalid::int
     , brontype::varchar(12)
     , bronactualiteit::date
     , bronbeschrijving::text
     , bronnauwkeurigheid::float4
     , mutatietype
     , typegebouw::text[]
     , fysiekvoorkomen::text
     , hoogteklasse::text
     , hoogteniveau::int2
     , status::text
     , soortnaam::text
     , naam::text[]
     , objectbegintijd::timestamp WITH TIME ZONE
     , tijdstipregistratie::timestamp WITH TIME ZONE
     , visualisatiecode::int2
     , tdncode::int2
     , st_force2d(wkb_geometry)::geometry(Polygon, 28992) AS geometrie_vlak
FROM ${gebouw_tbl}
WHERE geometrytype(wkb_geometry) = 'POLYGON';