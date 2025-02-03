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
WHERE begingeldigheid <= ${pelidatum}
  AND (eindgeldigheid ISNULL OR eindgeldigheid >= ${pelidatum})
  AND (tijdstipinactief ISNULL OR tijdstipinactief <= ${pelidatum})
  AND status <> 'Woonplaats ingetrokken';