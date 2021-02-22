-- assign tile to lbcs category
WITH similarities AS (SELECT
  grid.geo_id,
  MAX(udfs.cosine_similarity(tfidf.tfidf_vec, lbcs.tfidf_vec)) as max_similarity
FROM
  `gcp-pdp-osm-dev.osm_clustering_grid_01km.vectors_tfidf` tfidf
JOIN `gcp-pdp-osm-dev.osm_cities.cities_population_grid_01km` grid USING(geo_id)
CROSS JOIN `gcp-pdp-osm-dev.lbcs.lbcs_tfidf` lbcs
WHERE grid.city_name = "Madrid"
AND lbcs.dimension = 'Function'
AND udfs.cosine_similarity(tfidf.tfidf_vec, lbcs.tfidf_vec) > 0
GROUP BY grid.geo_id)
SELECT
  grid.geo_id,
  grid.geog,
  lbcs.name,
  lbcs.color,
  udfs.cosine_similarity(tfidf.tfidf_vec, lbcs.tfidf_vec) as similarity
FROM
  `gcp-pdp-osm-dev.osm_clustering_grid_01km.vectors_tfidf` tfidf
JOIN `gcp-pdp-osm-dev.osm_cities.cities_population_grid_01km` grid USING(geo_id)
CROSS JOIN `gcp-pdp-osm-dev.lbcs.lbcs_tfidf` lbcs
JOIN similarities ON similarities.geo_id = tfidf.geo_id AND similarities.max_similarity = udfs.cosine_similarity(tfidf.tfidf_vec, lbcs.tfidf_vec)
WHERE grid.city_name = "Madrid"
AND lbcs.dimension = 'Function'
ORDER BY similarity DESC

-- Selects tile terms
WITH objects_with_terms AS (SELECT osm_id, geometry, term
FROM `gcp-pdp-osm-dev.osm_cities.cities_objects` as objects
JOIN UNNEST(SPLIT(CONCAT(layer_class, "_", layer_name), "_")) as term
WHERE objects.city_name = 'Madrid')
, data AS (
SELECT
  grid.geo_id,
  objects.term
  FROM
  objects_with_terms AS objects,
  `gcp-pdp-osm-dev.osm_cities.cities_population_grid_01km` as grid
WHERE ST_INTERSECTS(grid.geog, objects.geometry)
)
, counts AS (SELECT
  geo_id,
  term,
  COUNT(term) OVER(partition by CONCAT(geo_id, term)) as term_count,
  COUNT(term) OVER(partition by geo_id) as terms_in_cell
FROM data)
, tf AS (SELECT geo_id, term, ANY_VALUE(term_count)/ANY_VALUE(terms_in_cell) as tf
FROM counts
GROUP BY geo_id, term)
, term_in_cells AS (
  SELECT term, COUNT(DISTINCT geo_id) in_cells
  FROM data
  GROUP BY 1
)
, total_cells AS (
  SELECT COUNT(DISTINCT geo_id) total_cells
  FROM data
)
, idf AS (
  SELECT term, LOG(total_cells.total_cells/in_cells) idf
  FROM term_in_cells
  CROSS JOIN total_cells
)
, tf_idf AS (
SELECT
geo_id,
term,
tf.tf * idf.idf tfidf,
CONCAT(term, ': ', CAST(tf.tf * idf.idf AS STRING)) as term_and_tfidf
FROM tf
JOIN idf
USING(term)
ORDER BY tfidf DESC
)
SELECT
  geo_id,
  ANY_VALUE(grid.geog) as geog,
  ARRAY_TO_STRING(ARRAY_AGG(term_and_tfidf ORDER BY tfidf DESC), ',<br/>') as terms
FROM tf_idf
JOIN `gcp-pdp-osm-dev.osm_cities.cities_population_grid_01km` as grid USING(geo_id)
WHERE grid.city_name = "Madrid"
GROUP BY geo_id