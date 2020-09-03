SELECT
    osmium_table.id,
    osmium_table.osm_version,
    osmium_table.username,
    osmium_table.changeset,
    osmium_table.visible,
    osmium_table.osm_timestamp,
    gdal_table.geometry,
    osmium_table.all_tags,
    osmium_table.nodes,
FROM
  {}.planet_ways AS osmium_table
LEFT JOIN
  {}.planet_features AS gdal_table
ON
  osmium_table.id = gdal_table.osm_id AND osmium_table.osm_timestamp = gdal_table.osm_timestamp