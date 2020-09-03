common_query() {
echo "
WITH osm AS (
  SELECT id, null AS way_id, osm_timestamp, version, all_tags FROM \`${BQ_DATASET_TO_EXPORT}.planet_nodes\`
  UNION ALL
  SELECT id, id AS way_id, osm_timestamp, version, all_tags FROM \`${BQ_DATASET_TO_EXPORT}.planet_ways\`
  UNION ALL
  SELECT id, null AS way_id, osm_timestamp, version, all_tags FROM \`${BQ_DATASET_TO_EXPORT}.planet_relations\`
)
SELECT $CODE AS layer_code, '$CLASS' AS layer_class, '$NAME_PREFIX$N' AS layer_name, f.feature_type AS gdal_type,
f.osm_id  AS osm_id,
f.osm_way_id AS osm_way_id,
f.osm_timestamp,
osm.version AS osm_version,
osm.all_tags,
f.geometry
FROM \`${BQ_DATASET_TO_EXPORT}.planet_features\` AS f, osm
WHERE osm.id = f.osm_id AND osm.osm_timestamp = f.osm_timestamp
$EXTRA_CONSTRAINTS

UNION ALL

SELECT $CODE AS layer_code, '$CLASS' AS layer_class, '$NAME_PREFIX$N' AS layer_name, f.feature_type AS gdal_type,
f.osm_id AS osm_id,
f.osm_way_id AS osm_way_id,
f.osm_timestamp,
osm.version AS osm_version,
osm.all_tags,
f.geometry
FROM \`${BQ_DATASET_TO_EXPORT}.planet_features\` AS f, osm
WHERE osm.way_id = f.osm_way_id AND osm.osm_timestamp = f.osm_timestamp
$EXTRA_CONSTRAINTS
"
}
