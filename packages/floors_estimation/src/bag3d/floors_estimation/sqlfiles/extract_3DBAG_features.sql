DROP TABLE
  IF EXISTS ${new_table};

CREATE TABLE
  ${new_table} (
    id VARCHAR,
    construction_year INT,
    roof_type VARCHAR,
    h_roof_50p FLOAT,
    h_roof_70p FLOAT,
    h_roof_max FLOAT,
    h_roof_min FLOAT,
    area_roof FLOAT,
    area_ext_walls FLOAT,
    area_party_walls FLOAT,
    area_ground FLOAT,
    volume_lod22 FLOAT,
    volume_lod12 FLOAT,
    PRIMARY KEY(id)
  );