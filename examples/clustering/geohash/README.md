## Prepare geohash level 1 grid for initial objects partitioning

We limit latitude from -85 to 85, to avoid bigquery polygons conversion issues.

Script `./generate_geohash_l1.py` - generates query for level 1 grid with planar polygons.
Insert result of this script into the BQ table `gcp-pdp-osm-dev.geohash_v1.level_1`


## Partition objects by level1 grid to optimize further processing  

Create `objects_partitioned` table partitioned by `geohash_l1_decimal` field. See schema:
```json
[
  {
    "mode": "NULLABLE",
    "name": "osm_id",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "geometry",
    "type": "GEOGRAPHY"
  },
  {
    "mode": "NULLABLE",
    "name": "term",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "geohash",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "geohash_l1_decimal",
    "type": "INTEGER"
  }
]
```

Fill table:
```sql
SELECT 
    objects.osm_id, objects.geometry, CONCAT(objects.layer_code, '_', objects.layer_class, "_", objects.layer_name) as term,
    grid.geohash, grid.geohash_l1_decimal
FROM `bigquery-public-data.geo_openstreetmap.planet_layers` as objects
JOIN `gcp-pdp-osm-dev:geohash_v1.level_1` grid
ON ST_INTERSECTS(objects.geometry, grid.geog)
```

## Count terms per grid cell

Start with
`gcp-pdp-osm-dev.geohash_v1.objects_partitioned`

Prepare:
`gcp-pdp-osm-dev:geohash_v1.level_7_terms`
- geohash
- term

Script `./count_terms.py` - generates bq commands to prepare `level_7_terms` data.

And
`gcp-pdp-osm-dev:geohash_v1.level_7_terms_counts`
- geohash
- term
- term_count
- terms_in_cell

```
SELECT
  geohash,
  term,
  COUNT(term) OVER(partition by CONCAT(geohash, term)) as term_count,
  COUNT(term) OVER(partition by geohash) as terms_in_cell
FROM `gcp-pdp-osm-dev.geohash_v1.level_7_terms`
```


## Calculate TF-IDF vectors 

write result to `gcp-pdp-osm-dev:geohash_v1.level_7_vectors_tfidf_raw`

```
WITH tf AS (SELECT geohash, term, ANY_VALUE(term_count)/ANY_VALUE(terms_in_cell) as tf
FROM `gcp-pdp-osm-dev.geohash_v1.level_7_terms_counts`
GROUP BY geohash, term)
, term_in_cells AS (
  SELECT term, COUNT(DISTINCT geohash) in_cells
  FROM `gcp-pdp-osm-dev.geohash_v1.level_7_terms`
  GROUP BY 1
)
, total_cells AS (
  SELECT COUNT(DISTINCT geohash) total_cells
  FROM `gcp-pdp-osm-dev.geohash_v1.level_7_terms`
)
, idf AS (
  SELECT term, LOG(total_cells.total_cells/in_cells) idf
  FROM term_in_cells
  CROSS JOIN total_cells
)
SELECT
    geohash,
    term,
    tf.tf * idf.idf tfidf,
CONCAT(term, ': ', CAST(tf.tf * idf.idf AS STRING)) as term_and_tfidf
FROM tf
JOIN idf
USING(term)
ORDER BY geohash, tfidf DESC
```
