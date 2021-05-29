WITH similarities AS (SELECT * FROM `gcp-pdp-osm-dev.geohash.level_7_u_w_lbcs_similarities`)
SELECT
  grid.geohash,
  grid.geog,
  lbcs_a.name as activity_name,
  lbcs_f.name as function_name,
  lbcs_colors.color,
  udfs.cosine_similarity(tfidf.tfidf_vec, lbcs_a.tfidf_vec) as similarity_a,
  udfs.cosine_similarity(tfidf.tfidf_vec, lbcs_f.tfidf_vec) as similarity_f
FROM
  `gcp-pdp-osm-dev.geohash.level_7_u_w_vectors_tfidf` tfidf
JOIN `gcp-pdp-osm-dev.geohash.level_7` grid USING(geohash)
CROSS JOIN `gcp-pdp-osm-dev.lbcs.lbcs_tfidf_layers` lbcs_f
CROSS JOIN `gcp-pdp-osm-dev.lbcs.lbcs_tfidf_layers` lbcs_a
JOIN similarities
  ON similarities.geohash = tfidf.geohash
  AND similarities.max_similarity_f = udfs.cosine_similarity(tfidf.tfidf_vec, lbcs_f.tfidf_vec)
  AND similarities.max_similarity_a = udfs.cosine_similarity(tfidf.tfidf_vec, lbcs_a.tfidf_vec)
JOIN `gcp-pdp-osm-dev.lbcs.lbcs_activity_and_function` lbcs_colors
  ON lbcs_colors.activity = lbcs_a.name
  AND lbcs_colors.function = lbcs_f.name
WHERE lbcs_f.dimension = 'Function'
AND lbcs_a.dimension = 'Activity'
ORDER BY similarity_a, similarity_f DESC