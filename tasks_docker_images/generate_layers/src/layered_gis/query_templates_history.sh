common_query() {
echo "
WITH osm AS (
  SELECT id, null AS way_id, all_tags, osm_timestamp, geometry FROM \`${PROJECT_ID}.${BQ_DATASET_TO_EXPORT}.nodes\`
  UNION ALL
  SELECT id, id AS way_id, all_tags, osm_timestamp, geometry FROM \`${PROJECT_ID}.${BQ_DATASET_TO_EXPORT}.ways\`
  UNION ALL
  SELECT id, null AS way_id, all_tags, osm_timestamp, geometry FROM \`${PROJECT_ID}.${BQ_DATASET_TO_EXPORT}.relations\`
)
SELECT
$CODE AS layer_code,
'$CLASS' AS layer_class,
'$NAME_PREFIX$N' AS layer_name,
osm.id  AS osm_id,
osm.way_id AS osm_way_id,
osm.osm_timestamp AS osm_timestamp,
osm.all_tags,
osm.geometry
FROM osm
WHERE osm.id IS NOT NULL
$EXTRA_CONSTRAINTS
"
}
