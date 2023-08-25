CREATE INDEX lod12_2d_geom_idx ON { schema }.lod12_2d USING gist (geom);

CREATE INDEX lod13_2d_geom_idx ON { schema }.lod13_2d USING gist (geom);

CREATE INDEX lod13_2d_geom_idx ON { schema }.lod22_2d USING gist (geom);

ALTER TABLE
    { schema }.lod12_2d
ADD
    COLUMN dak_type VARCHAR;

UPDATE
    { schema }.lod12_2d ld
SET
    dak_type = p.b3_dak_type
FROM
    { schema }.pand p
WHERE
    ld.identificatie = p.identificatie;

ALTER TABLE
    { schema }.lod12_2d
ADD
    COLUMN h_maaiveld FLOAT8;

UPDATE
    { schema }.lod12_2d ld
SET
    h_maaiveld = p.b3_h_maaiveld
FROM
    { schema }.pand p
WHERE
    ld.identificatie = p.identificatie;

ALTER TABLE
    { schema }.lod13_2d
ADD
    COLUMN dak_type VARCHAR;

UPDATE
    { schema }.lod13_2d ld
SET
    dak_type = p.b3_dak_type
FROM
    { schema }.pand p
WHERE
    ld.identificatie = p.identificatie;

ALTER TABLE
    { schema }.lod13_2d
ADD
    COLUMN h_maaiveld FLOAT8;

UPDATE
    { schema }.lod13_2d ld
SET
    h_maaiveld = p.b3_h_maaiveld
FROM
    { schema }.pand p
WHERE
    ld.identificatie = p.identificatie;

ALTER TABLE
    { schema }.lod22_2d
ADD
    COLUMN dak_type VARCHAR;

UPDATE
    { schema }.lod22_2d ld
SET
    dak_type = p.b3_dak_type
FROM
    { schema }.pand p
WHERE
    ld.identificatie = p.identificatie;

ALTER TABLE
    { schema }.lod22_2d
ADD
    COLUMN h_maaiveld FLOAT8;

UPDATE
    { schema }.lod22_2d ld
SET
    h_maaiveld = p.b3_h_maaiveld
FROM
    { schema }.pand p
WHERE
    ld.identificatie = p.identificatie;