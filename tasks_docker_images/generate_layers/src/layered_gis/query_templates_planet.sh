common_query() {
echo "
WITH osm AS (
  SELECT id, null AS way_id, all_tags FROM \`${PROJECT_ID}.${BQ_DATASET_TO_EXPORT}.nodes\`
  UNION ALL
  SELECT id, id AS way_id, all_tags FROM \`${PROJECT_ID}.${BQ_DATASET_TO_EXPORT}.ways\`
  UNION ALL
  SELECT id, null AS way_id, all_tags FROM \`${PROJECT_ID}.${BQ_DATASET_TO_EXPORT}.relations\`
)
SELECT $CODE AS layer_code, '$CLASS' AS layer_class, '$NAME_PREFIX$N' AS layer_name, f.feature_type AS gdal_type,
f.osm_id  AS osm_id,
f.osm_way_id AS osm_way_id,
f.osm_timestamp,
osm.all_tags,
f.geometry
FROM \`${PROJECT_ID}.${BQ_DATASET_TO_EXPORT}.feature_union\` AS f, osm
WHERE osm.id = f.osm_id
$EXTRA_CONSTRAINTS

UNION ALL

SELECT $CODE AS layer_code, '$CLASS' AS layer_class, '$NAME_PREFIX$N' AS layer_name, f.feature_type AS gdal_type,
f.osm_id AS osm_id,
f.osm_way_id AS osm_way_id,
f.osm_timestamp,
osm.all_tags,
f.geometry
FROM \`${PROJECT_ID}.${BQ_DATASET_TO_EXPORT}.feature_union\` AS f, osm
WHERE osm.way_id = f.osm_way_id
$EXTRA_CONSTRAINTS
"
}
