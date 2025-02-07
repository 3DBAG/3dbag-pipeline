DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
SELECT ogc_fid      AS fid
     , naam
     , type
     , woonplaatsref
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
FROM ${opr_tbl}
WHERE begingeldigheid <= ${reference_date}
  AND (eindgeldigheid ISNULL OR eindgeldigheid >= ${reference_date})
  AND (tijdstipinactief ISNULL OR tijdstipinactief <= ${reference_date})
  AND status <> 'Naamgeving ingetrokken';