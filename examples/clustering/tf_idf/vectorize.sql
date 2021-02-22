WITH objects_with_terms AS (SELECT osm_id, geometry, term
FROM `gcp-pdp-osm-dev.osm_cities.cities_objects` as objects
JOIN UNNEST(SPLIT(CONCAT(layer_class, "_", layer_name), "_")) as term
WHERE objects.city_name = 'Kyiv')
, data AS (
SELECT
  grid.geo_id,
  objects.term
  FROM
  objects_with_terms AS objects,
  `gcp-pdp-osm-dev.osm_cities.cities_population_grid_1km` as grid
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
ORDER BY geo_id, tfidf DESC
)
, features_matrix AS (SELECT geo_id, word
FROM `gcp-pdp-osm-dev.words.w2v_glove_6B_300d_osm_tags`
CROSS JOIN (SELECT geo_id FROM data GROUP BY geo_id)
ORDER BY geo_id, word)
SELECT
  fm.geo_id, ARRAY_AGG(fm.word ORDER BY fm.word) as words, ARRAY_AGG(IFNULL(tf_idf.tfidf, 0.0) ORDER BY fm.word) as tfidf_vec
FROM features_matrix fm
LEFT JOIN tf_idf ON tf_idf.term = fm.word AND tf_idf.geo_id = fm.geo_id
GROUP BY geo_id
ORDER BY geo_id