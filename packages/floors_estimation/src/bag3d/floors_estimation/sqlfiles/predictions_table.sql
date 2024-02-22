DROP TABLE
  IF EXISTS ${predictions_table};

CREATE TABLE
  ${predictions_table}(
    identificatie VARCHAR,
    b3_bouwlagen INTEGER,
    PRIMARY KEY(identificatie)
  )