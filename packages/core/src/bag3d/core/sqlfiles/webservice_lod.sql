BEGIN;
CREATE INDEX lod12_2d_id_idx ON ${lod12_2d_tmp} USING btree (identificatie);

CREATE INDEX lod13_2d_id_idx ON ${lod13_2d_tmp} USING btree (identificatie);

CREATE INDEX lod22_2d_id_idx ON ${lod22_2d_tmp} USING btree (identificatie);

CREATE INDEX pand_id_idx ON ${pand_table} USING btree (identificatie);
COMMIT;

BEGIN;
DROP TABLE IF EXISTS ${lod12_2d};
CREATE TABLE ${lod12_2d} AS
SELECT
    ld.*,
    p.b3_bag_bag_overlap,
    p.b3_n_vlakken,
    p.b3_pw_onvoldoende,
    p.b3_t_run,
    p.b3_succes,
    p.b3_extrusie
    p.b3_is_glas_dak,
    p.b3_dak_type,
    p.b3_h_maaiveld,
    p.b3_kas_warenhuis,
    p.b3_mutatie_ahn3_ahn4,
    p.b3_mutatie_ahn4_ahn5,
    p.b3_nodata_fractie_ahn3,
    p.b3_nodata_fractie_ahn4,
    p.b3_nodata_fractie_ahn5,
    p.b3_nodata_radius_ahn3,
    p.b3_nodata_radius_ahn4,
    p.b3_nodata_radius_ahn5,
    p.b3_opp_buitenmuur,
    p.b3_opp_dak_plat,
    p.b3_opp_dak_schuin,
    p.b3_opp_grond,
    p.b3_opp_scheidingsmuur,
    p.b3_puntdichtheid_ahn3,
    p.b3_puntdichtheid_ahn4,
    p.b3_puntdichtheid_ahn5,
    p.b3_pw_bron,
    p.b3_pw_datum,
    p.b3_pw_selectie_reden,
    p.b3_rmse_lod12,
    p.b3_rmse_lod13,
    p.b3_rmse_lod22,
    p.b3_val3dity_lod12,
    p.b3_val3dity_lod13,
    p.b3_val3dity_lod22,
    p.b3_volume_lod12,
    p.b3_volume_lod13,
    p.b3_volume_lod22,
    p.b3_bouwlagen,
	p.b3_kwaliteitsindicator,
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
    ${lod12_2d_tmp} ld
    JOIN ${pand_table} p 
    ON p.identificatie = ld.identificatie;

ALTER TABLE ${lod12_2d} ADD PRIMARY KEY (fid);

DROP TABLE IF EXISTS ${lod13_2d};
CREATE TABLE ${lod13_2d} AS
SELECT
    ld.*,
    p.b3_bag_bag_overlap,
    p.b3_n_vlakken,
    p.b3_pw_onvoldoende,
    p.b3_t_run,
    p.b3_succes,
    p.b3_extrusie
    p.b3_is_glas_dak,
    p.b3_dak_type,
    p.b3_h_maaiveld,
    p.b3_kas_warenhuis,
    p.b3_mutatie_ahn3_ahn4,
    p.b3_mutatie_ahn4_ahn5,
    p.b3_nodata_fractie_ahn3,
    p.b3_nodata_fractie_ahn4,
    p.b3_nodata_fractie_ahn5,
    p.b3_nodata_radius_ahn3,
    p.b3_nodata_radius_ahn4,
    p.b3_nodata_radius_ahn5,
    p.b3_opp_buitenmuur,
    p.b3_opp_dak_plat,
    p.b3_opp_dak_schuin,
    p.b3_opp_grond,
    p.b3_opp_scheidingsmuur,
    p.b3_puntdichtheid_ahn3,
    p.b3_puntdichtheid_ahn4,
    p.b3_puntdichtheid_ahn5,
    p.b3_pw_bron,
    p.b3_pw_datum,
    p.b3_pw_selectie_reden,
    p.b3_rmse_lod12,
    p.b3_rmse_lod13,
    p.b3_rmse_lod22,
    p.b3_val3dity_lod12,
    p.b3_val3dity_lod13,
    p.b3_val3dity_lod22,
    p.b3_volume_lod12,
    p.b3_volume_lod13,
    p.b3_volume_lod22,
    p.b3_bouwlagen,
	p.b3_kwaliteitsindicator,
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
    ${lod13_2d_tmp} ld
    JOIN ${pand_table} p 
    ON p.identificatie = ld.identificatie;

ALTER TABLE ${lod13_2d} ADD PRIMARY KEY (fid);

DROP TABLE IF EXISTS ${lod22_2d};
CREATE TABLE ${lod22_2d} AS
SELECT
    ld.*,
    p.b3_bag_bag_overlap,
    p.b3_n_vlakken,
    p.b3_pw_onvoldoende,
    p.b3_t_run,
    p.b3_succes,
    p.b3_extrusie
    p.b3_is_glas_dak,
    p.b3_dak_type,
    p.b3_h_maaiveld,
    p.b3_kas_warenhuis,
    p.b3_mutatie_ahn3_ahn4,
    p.b3_mutatie_ahn4_ahn5,
    p.b3_nodata_fractie_ahn3,
    p.b3_nodata_fractie_ahn4,
    p.b3_nodata_fractie_ahn5,
    p.b3_nodata_radius_ahn3,
    p.b3_nodata_radius_ahn4,
    p.b3_nodata_radius_ahn5,
    p.b3_opp_buitenmuur,
    p.b3_opp_dak_plat,
    p.b3_opp_dak_schuin,
    p.b3_opp_grond,
    p.b3_opp_scheidingsmuur,
    p.b3_puntdichtheid_ahn3,
    p.b3_puntdichtheid_ahn4,
    p.b3_puntdichtheid_ahn5,
    p.b3_pw_bron,
    p.b3_pw_datum,
    p.b3_pw_selectie_reden,
    p.b3_rmse_lod12,
    p.b3_rmse_lod13,
    p.b3_rmse_lod22,
    p.b3_val3dity_lod12,
    p.b3_val3dity_lod13,
    p.b3_val3dity_lod22,
    p.b3_volume_lod12,
    p.b3_volume_lod13,
    p.b3_volume_lod22,
    p.b3_bouwlagen,
	p.b3_kwaliteitsindicator,
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
    ${lod22_2d_tmp} ld
    JOIN ${pand_table} p 
    ON p.identificatie = ld.identificatie;

ALTER TABLE ${lod22_2d} ADD PRIMARY KEY (fid);

COMMIT;

BEGIN;
CREATE INDEX lod12_geom_idx ON ${lod12_2d} USING gist (geom);
CREATE INDEX lod13_geom_idx ON ${lod13_2d} USING gist (geom);
CREATE INDEX lod22_geom_idx ON ${lod22_2d} USING gist (geom);
COMMIT;
