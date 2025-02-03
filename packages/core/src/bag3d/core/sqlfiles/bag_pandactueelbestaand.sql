DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
WITH repair AS (SELECT ogc_fid                                                AS fid
                     , oorspronkelijkbouwjaar
                     , identificatie
                     , status
                     , geconstateerd
                     , documentdatum
                     , documentnummer
                     , voorkomenidentificatie
                     , begingeldigheid
                     , eindgeldigheid
                     , tijdstipregistratie
                     , eindregistratie
                     , tijdstipinactief
                     , tijdstipregistratielv
                     , tijdstipeindregistratielv
                     , tijdstipinactieflv
                     , tijdstipnietbaglv
                     , st_makevalid((st_dump(st_force2d(wkb_geometry))).geom) AS geometrie
                FROM ${pand_tbl}
                WHERE begingeldigheid <= ${pelidatum}
                  AND (eindgeldigheid IS NULL OR eindgeldigheid >= ${pelidatum})
                  AND (tijdstipinactief ISNULL OR tijdstipinactief <= ${pelidatum})
                  AND (status <> 'Niet gerealiseerd pand'
                    AND status <> 'Pand gesloopt'
                    AND status <> 'Bouwvergunning verleend'))
   , duplicates AS (SELECT fid
                         , oorspronkelijkbouwjaar
                         , identificatie
                         , status
                         , geconstateerd
                         , documentdatum
                         , documentnummer
                         , voorkomenidentificatie
                         , begingeldigheid
                         , eindgeldigheid
                         , tijdstipregistratie
                         , eindregistratie
                         , tijdstipinactief
                         , tijdstipregistratielv
                         , tijdstipeindregistratielv
                         , tijdstipinactieflv
                         , tijdstipnietbaglv
                         , geometrie::geometry(Polygon, 28992) AS         geometrie
                         , ROW_NUMBER() OVER (PARTITION BY identificatie) rn
                    FROM repair
                    WHERE GeometryType(geometrie) = 'POLYGON')
SELECT fid
     , oorspronkelijkbouwjaar
     , identificatie
     , status
     , geconstateerd
     , documentdatum
     , documentnummer
     , voorkomenidentificatie
     , begingeldigheid
     , eindgeldigheid
     , tijdstipregistratie
     , eindregistratie
     , tijdstipinactief
     , tijdstipregistratielv
     , tijdstipeindregistratielv
     , tijdstipinactieflv
     , tijdstipnietbaglv
     , geometrie
FROM duplicates
WHERE rn = 1;