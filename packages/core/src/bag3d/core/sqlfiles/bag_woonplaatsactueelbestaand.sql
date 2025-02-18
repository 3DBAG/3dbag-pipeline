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
WHERE (tijdstipinactieflv > ${reference_date} OR tijdstipinactieflv ISNULL)
  AND (tijdstipnietbaglv > ${reference_date} OR tijdstipnietbaglv ISNULL)
  AND (tijdstipregistratielv <= ${reference_date} AND
       (tijdstipeindregistratielv > ${reference_date} OR
        tijdstipeindregistratielv ISNULL))
  AND (begingeldigheid <= ${reference_date} AND
       (eindgeldigheid = begingelidgheid OR eindgeldigheid > ${reference_date} OR
        eindgeldigheid ISNULL))
  AND status <> 'Woonplaats ingetrokken';