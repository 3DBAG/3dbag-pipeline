DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
SELECT ogc_fid      AS fid
     , gebruiksdoel
     , oppervlakte
     , hoofdadresnummeraanduidingref
     , nevenadresnummeraanduidingref
     , pandref
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
     , wkb_geometry AS geometrie
FROM ${vbo_tbl}
WHERE begingeldigheid <= NOW()
  AND (eindgeldigheid ISNULL OR eindgeldigheid >= NOW())
  AND (tijdstipinactief ISNULL OR tijdstipinactief <= NOW())
  AND (status <> 'Niet gerealiseerd verblijfsobject'
    AND status <> 'Verblijfsobject ingetrokkent'
    AND status <> 'Verblijfsobject buiten gebruik'
    AND status <> 'Verblijfsobject ten onrechte opgevoerd');