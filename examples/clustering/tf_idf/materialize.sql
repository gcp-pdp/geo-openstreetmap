-- Selects geo ID, geography, TF-IDF vector, TF-IDF features,
-- lbcs category name, lbcs color, similarity with lbcs category
WITH features AS (SELECT ARRAY_AGG(word ORDER BY word) as words
FROM `gcp-pdp-osm-dev.words.w2v_glove_6B_300d_osm_tags`)
,similarities AS (SELECT
  grid.geo_id,
  MAX(udfs.cosine_similarity(tfidf.tfidf_vec, lbcs.tfidf_vec)) as max_similarity
FROM
  `gcp-pdp-osm-dev.osm_clustering_grid_01km.vectors_tfidf` tfidf
JOIN `gcp-pdp-osm-dev.osm_cities.cities_population_grid_01km` grid USING(geo_id)
CROSS JOIN `gcp-pdp-osm-dev.lbcs.lbcs_tfidf` lbcs
WHERE lbcs.dimension = 'Function'
AND udfs.cosine_similarity(tfidf.tfidf_vec, lbcs.tfidf_vec) > 0
GROUP BY grid.geo_id)
SELECT
  grid.geo_id,
  grid.geog,
  grid.city_name,
  tfidf.tfidf_vec,
  features.words as tfidf_features,
  lbcs.name,
  lbcs.color,
  udfs.cosine_similarity(tfidf.tfidf_vec, lbcs.tfidf_vec) as similarity
FROM
  `gcp-pdp-osm-dev.osm_clustering_grid_01km.vectors_tfidf` tfidf
JOIN `gcp-pdp-osm-dev.osm_cities.cities_population_grid_01km` grid USING(geo_id)
CROSS JOIN features
CROSS JOIN `gcp-pdp-osm-dev.lbcs.lbcs_tfidf` lbcs
JOIN similarities ON similarities.geo_id = tfidf.geo_id AND similarities.max_similarity = udfs.cosine_similarity(tfidf.tfidf_vec, lbcs.tfidf_vec)
AND lbcs.dimension = 'Function'
