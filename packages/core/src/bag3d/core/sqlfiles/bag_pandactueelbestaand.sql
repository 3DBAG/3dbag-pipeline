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
                WHERE (tijdstipinactieflv > ${reference_date} OR
                       tijdstipinactieflv ISNULL)
                  AND (tijdstipnietbaglv > ${reference_date} OR
                       tijdstipnietbaglv ISNULL)
                  AND (tijdstipregistratielv <= ${reference_date} AND
                       (tijdstipeindregistratielv > ${reference_date} OR
                        tijdstipeindregistratielv ISNULL))
                  AND (begingeldigheid <= ${reference_date} AND
                       (eindgeldigheid = begingeldigheid OR
                        eindgeldigheid > ${reference_date} OR eindgeldigheid ISNULL))
                  AND (status <> 'Niet gerealiseerd pand' AND
                       status <> 'Pand gesloopt' AND
                       status <> 'Bouwvergunning verleend'))
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