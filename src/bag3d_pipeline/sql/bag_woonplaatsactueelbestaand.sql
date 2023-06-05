DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
SELECT ogc_fid      AS fid
     , naam
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
FROM ${wpl_tbl}
WHERE begingeldigheid <= NOW()
  AND (eindgeldigheid ISNULL OR eindgeldigheid >= NOW())
  AND (tijdstipinactief ISNULL OR tijdstipinactief <= NOW())
  AND status <> 'Woonplaats ingetrokken';