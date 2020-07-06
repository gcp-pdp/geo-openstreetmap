
WITH osm AS (
  SELECT id, null AS way_id, all_tags, osm_timestamp, geometry FROM `gcp-pdp-osm-dev.osm_to_bq_historyeast1.nodes`
  UNION ALL
  SELECT id, id AS way_id, all_tags, osm_timestamp, geometry FROM `gcp-pdp-osm-dev.osm_to_bq_historyeast1.ways`
  UNION ALL
  SELECT id, null AS way_id, all_tags, osm_timestamp, geometry FROM `gcp-pdp-osm-dev.osm_to_bq_historyeast1.relations`
)
SELECT
5233 AS layer_code,
'traffic' AS layer_class,
'calming_table' AS layer_name,
osm.id  AS osm_id,
osm.way_id AS osm_way_id,
osm.osm_timestamp AS osm_timestamp,
osm.all_tags,
osm.geometry
FROM osm
WHERE osm_id IS NOT NULL
AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'traffic_calming' AND tags.value='table')

