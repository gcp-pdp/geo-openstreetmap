CREATE OR REPLACE TABLE `openstreetmap-public-data-dev.osm_planet.features` AS

SELECT 'line' AS feature_type, osm_id, osm_way_id, osm_version, osm_timestamp, all_tags, geometry 
FROM `openstreetmap-public-data-dev.geo_layered_gis.lines`

UNION ALL

SELECT 'multilinestring' AS feature_type, osm_id, osm_way_id, osm_version, osm_timestamp, all_tags, geometry 
FROM `openstreetmap-public-data-dev.geo_layered_gis.multilinestrings`

UNION ALL

SELECT 'multipolygon' AS feature_type, osm_id, osm_way_id, osm_version, osm_timestamp, all_tags, geometry 
FROM `openstreetmap-public-data-dev.geo_layered_gis.multipolygons`

UNION ALL

SELECT 'other_relation' AS feature_type, osm_id, osm_way_id, osm_version, osm_timestamp, all_tags, geometry 
FROM `openstreetmap-public-data-dev.geo_layered_gis.other_relations`

UNION ALL

SELECT 'point' AS feature_type, osm_id, osm_way_id, osm_version, osm_timestamp, all_tags, geometry 
FROM `openstreetmap-public-data-dev.geo_layered_gis.points`
