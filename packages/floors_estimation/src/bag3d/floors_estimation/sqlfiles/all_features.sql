DROP TABLE IF EXISTS ${all_features};
CREATE TABLE
  ${all_features} AS (
    SELECT
      bf.identificatie,
      bf.no_vertices,
      bf.perimeter,
      bf.no_units,
      bf.net_area,
      bf.building_function,
      bf.no_neighbours_100,
      bf.no_adjacent_neighbours,
      bf.cbs_percent_multihousehold,
      bf.cbs_pop_per_km2,
      bf.cbs_dist_to_horeca,
      bf.buildingtype,
      bfd.construction_year,
      (
        CASE
          WHEN bfd.roof_type = 'horizontal' THEN 0
          WHEN bfd.roof_type = 'multiple horizontal' THEN 0
          WHEN bfd.roof_type = 'slanted' THEN 1
          else NULL
        end
      ) AS roof_type,
      bfd.h_roof_50p,
      bfd.h_roof_70p,
      bfd.h_roof_max,
      bfd.h_roof_min,
      bfd.area_roof,
      bfd.area_ext_walls,
      bfd.area_party_walls,
      bfd.area_ground,
      bfd.volume_lod22,
      bfd.volume_lod12
    FROM
      ${external_features} bf
      JOIN ${bag3d_features} bfd ON bf.identificatie = bfd.id
    WHERE
      bf.building_function = 0
      or bf.building_function = 1
  );