DROP TABLE IF EXISTS ${new_table} CASCADE;

CREATE TABLE ${new_table} (
    id serial PRIMARY KEY,
    geom geometry(Polygon, 28992)
);
