
CREATE INDEX lod12_2d_id_idx ON ${lod12_table} USING btree (identificatie);

CREATE INDEX lod13_2d_id_idx ON ${lod13_table} USING btree (identificatie);

CREATE INDEX lod22_2d_id_idx ON ${lod22_table} USING btree (identificatie);

CREATE INDEX pand_id_idx ON ${pand_table} USING btree (identificatie);

DROP MATERIALIZED VIEW IF EXISTS ${mat_view_lod12};
CREATE materialized VIEW ${mat_view_lod12} AS
SELECT
    ld.*,
    p.b3_dak_type,
    p.b3_h_maaiveld,
    p.b3_kas_warenhuis,
    p.b3_mutatie_ahn3_ahn4,
    p.b3_nodata_fractie_ahn3,
    p.b3_nodata_fractie_ahn4,
    p.b3_nodata_radius_ahn3,
    p.b3_nodata_radius_ahn4,
    p.b3_puntdichtheid_ahn3,
    p.b3_puntdichtheid_ahn4,
    p.b3_pw_bron,
    p.b3_pw_datum,
    p.b3_pw_selectie_reden,
    p.b3_reconstructie_onvolledig,
    p.b3_rmse_lod12,
    p.b3_rmse_lod13,
    p.b3_rmse_lod22,
    p.b3_val3dity_lod12,
    p.b3_val3dity_lod13,
    p.b3_val3dity_lod22,
    p.b3_volume_lod12,
    p.b3_volume_lod13,
    p.b3_volume_lod22,
    p.begingeldigheid,
    p.documentdatum,
    p.documentnummer,
    p.eindgeldigheid,
    p.eindregistratie,
    p.geconstateerd,
    p.oorspronkelijkbouwjaar,
    p.status,
    p.tijdstipeindregistratielv,
    p.tijdstipinactief,
    p.tijdstipinactieflv,
    p.tijdstipnietbaglv,
    p.tijdstipregistratie,
    p.tijdstipregistratielv,
    p.voorkomenidentificatie
FROM
    ${lod12_table} ld
    JOIN ${pand_table} p 
    ON p.identificatie = ld.identificatie;

DROP MATERIALIZED VIEW IF EXISTS ${mat_view_lod13};
CREATE materialized VIEW ${mat_view_lod13} AS
SELECT
    ld.*,
    p.b3_dak_type,
    p.b3_h_maaiveld,
    p.b3_kas_warenhuis,
    p.b3_mutatie_ahn3_ahn4,
    p.b3_nodata_fractie_ahn3,
    p.b3_nodata_fractie_ahn4,
    p.b3_nodata_radius_ahn3,
    p.b3_nodata_radius_ahn4,
    p.b3_puntdichtheid_ahn3,
    p.b3_puntdichtheid_ahn4,
    p.b3_pw_bron,
    p.b3_pw_datum,
    p.b3_pw_selectie_reden,
    p.b3_reconstructie_onvolledig,
    p.b3_rmse_lod12,
    p.b3_rmse_lod13,
    p.b3_rmse_lod22,
    p.b3_val3dity_lod12,
    p.b3_val3dity_lod13,
    p.b3_val3dity_lod22,
    p.b3_volume_lod12,
    p.b3_volume_lod13,
    p.b3_volume_lod22,
    p.begingeldigheid,
    p.documentdatum,
    p.documentnummer,
    p.eindgeldigheid,
    p.eindregistratie,
    p.geconstateerd,
    p.oorspronkelijkbouwjaar,
    p.status,
    p.tijdstipeindregistratielv,
    p.tijdstipinactief,
    p.tijdstipinactieflv,
    p.tijdstipnietbaglv,
    p.tijdstipregistratie,
    p.tijdstipregistratielv,
    p.voorkomenidentificatie
FROM
    ${lod13_table} ld
    JOIN ${pand_table} p 
    ON p.identificatie = ld.identificatie;

DROP MATERIALIZED VIEW IF EXISTS ${mat_view_lod22};
CREATE materialized VIEW ${mat_view_lod22} AS
SELECT
    ld.*,
    p.b3_dak_type,
    p.b3_h_maaiveld,
    p.b3_kas_warenhuis,
    p.b3_mutatie_ahn3_ahn4,
    p.b3_nodata_fractie_ahn3,
    p.b3_nodata_fractie_ahn4,
    p.b3_nodata_radius_ahn3,
    p.b3_nodata_radius_ahn4,
    p.b3_puntdichtheid_ahn3,
    p.b3_puntdichtheid_ahn4,
    p.b3_pw_bron,
    p.b3_pw_datum,
    p.b3_pw_selectie_reden,
    p.b3_reconstructie_onvolledig,
    p.b3_rmse_lod12,
    p.b3_rmse_lod13,
    p.b3_rmse_lod22,
    p.b3_val3dity_lod12,
    p.b3_val3dity_lod13,
    p.b3_val3dity_lod22,
    p.b3_volume_lod12,
    p.b3_volume_lod13,
    p.b3_volume_lod22,
    p.begingeldigheid,
    p.documentdatum,
    p.documentnummer,
    p.eindgeldigheid,
    p.eindregistratie,
    p.geconstateerd,
    p.oorspronkelijkbouwjaar,
    p.status,
    p.tijdstipeindregistratielv,
    p.tijdstipinactief,
    p.tijdstipinactieflv,
    p.tijdstipnietbaglv,
    p.tijdstipregistratie,
    p.tijdstipregistratielv,
    p.voorkomenidentificatie
FROM
    ${lod22_table} ld
    JOIN ${pand_table} p 
    ON p.identificatie = ld.identificatie;


CREATE INDEX lod12_geom_idx ON ${mat_view_lod12} USING gist (geom);
CREATE INDEX lod13_geom_idx ON ${mat_view_lod13} USING gist (geom);
CREATE INDEX lod13_geom_idx ON ${mat_view_lod22} USING gist (geom);
