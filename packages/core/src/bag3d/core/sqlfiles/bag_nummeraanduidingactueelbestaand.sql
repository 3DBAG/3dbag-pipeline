DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} AS
SELECT ogc_fid      AS fid
     , huisnummer
     , huisletter
     , huisnummertoevoeging
     , postcode
     , typeadresseerbaarobject
     , openbareruimteref
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
FROM ${num_tbl}
WHERE (tijdstipinactieflv > ${reference_date} OR tijdstipinactieflv ISNULL)
  AND (tijdstipnietbaglv > ${reference_date} OR tijdstipnietbaglv ISNULL)
  AND (tijdstipregistratielv <= ${reference_date} AND
       (tijdstipeindregistratielv > ${reference_date} OR
        tijdstipeindregistratielv ISNULL))
  AND (begingeldigheid <= ${reference_date} AND
       (eindgeldigheid = begingeldigheid OR eindgeldigheid > ${reference_date} OR
        eindgeldigheid ISNULL))
  AND status <> 'Naamgeving ingetrokken';