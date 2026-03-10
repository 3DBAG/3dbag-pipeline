CREATE OR REPLACE VIEW ${view_single} AS
WITH joined AS (SELECT p.identificatie
                     , p.oorspronkelijkbouwjaar
                     , p.status
                     , vbo.gebruiksdoel
                     , vbo.oppervlakte
                     , vbo.vbo_identificatie
                     , p.geometrie
                FROM ${bag_pand} AS p
                         RIGHT JOIN (SELECT unnest(pandref) AS pandref
                                          , gebruiksdoel
                                          , oppervlakte
                                          , identificatie   AS vbo_identificatie
                                     FROM ${bag_vbo}
                                     WHERE 'woonfunctie' = ANY (gebruiksdoel)
                                       AND status IS NOT NULL) AS vbo
                                    ON vbo.pandref = p.identificatie)
SELECT *
FROM joined
         RIGHT JOIN LATERAL (
    SELECT identificatie
    FROM joined
    GROUP BY identificatie
    HAVING count(*) = 1) AS sub USING (identificatie);

COMMENT ON VIEW ${view_single} IS 'The lvbag.pandactueelbestaand objects that have a single verblijfsobject in them and the VBO gebruiksdoel contains woonfunctie.';

CREATE OR REPLACE VIEW ${view_multi} AS
WITH joined AS (SELECT p.identificatie
                     , p.oorspronkelijkbouwjaar
                     , p.status
                     , vbo.gebruiksdoel
                     , vbo.oppervlakte
                     , vbo.vbo_identificatie
                     , p.geometrie
                FROM ${bag_pand} AS p
                         RIGHT JOIN (SELECT unnest(pandref) AS pandref
                                          , gebruiksdoel
                                          , oppervlakte
                                          , identificatie   AS vbo_identificatie
                                     FROM ${bag_vbo}
                                     WHERE 'woonfunctie' = ANY (gebruiksdoel)
                                       AND status IS NOT NULL) AS vbo
                                    ON vbo.pandref = p.identificatie)
SELECT *
FROM joined
         RIGHT JOIN LATERAL (
    SELECT identificatie
    FROM joined
    GROUP BY identificatie
    HAVING count(*) > 1) AS sub USING (identificatie);

COMMENT ON VIEW ${view_multi} IS 'The lvbag.pandactueelbestaand objects that have multiple verblijfsobject in them and the VBO gebruiksdoel contains woonfunctie.';

CREATE OR REPLACE VIEW ${view_woonfunctie} AS
SELECT p.identificatie AS pand_identificatie
     , p.oorspronkelijkbouwjaar
     , vbo.oppervlakte
     , vbo.vbo_identificatie
     , p.geometrie
FROM ${bag_pand} AS p
         RIGHT JOIN (SELECT unnest(pandref) AS pandref
                          , gebruiksdoel
                          , oppervlakte
                          , identificatie   AS vbo_identificatie
                     FROM ${bag_vbo}
                     WHERE 'woonfunctie' = ANY (gebruiksdoel)
                       AND status IS NOT NULL) AS vbo
                    ON vbo.pandref = p.identificatie;

COMMENT ON VIEW ${view_woonfunctie} IS 'The lvbag.pandactueelbestaand objects joined with the VBO where the gebruiksdoel contains woonfunctie.';
