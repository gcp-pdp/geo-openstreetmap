## Import Glove vectors into BigQuery
Download word2vec (Glove)
```
cd ./data
wget http://nlp.stanford.edu/data/glove.6B.zip
unzip ./glove.6B.zip
rm ./glove.6B.zip
cd ../
```

Convert word2vec to JSONL format
```
cat ./data/glove.6B.300d.txt | python3 w2v_to_jsonl.py > ./data/glove.6B.300d.jsonl
```

Upload result to GCS:
```
gsutil cp ./data/glove.6B.300d.jsonl gs://gcp-pdp-osm-dev-bq-import/glove/
```

Import into BQ:
```
bq load \
 --source_format=NEWLINE_DELIMITED_JSON \
 gcp-pdp-osm-dev:osm_clustering.w2v_glove_6B_300d \
 gs://gcp-pdp-osm-dev-bq-import/glove.6B.300d.jsonl \
 "$(python3 w2v_generate_schema.py 300)"
```

## Prepare grid

```sql
SELECT
  *
FROM `bigquery-public-data.worldpop.population_grid_1km` AS grid,
gcp-pdp-osm-dev.osm_clustering.cities_circles AS cities
WHERE last_updated = '2020-01-01'
AND ST_DWITHIN(cities.center, grid.geog, cities.radius)
```

## Create model

```sql
CREATE OR REPLACE MODEL
  osm_clustering.grid_05km_300d_clusters_10 OPTIONS(model_type='kmeans', num_clusters=10, max_iterations=50, EARLY_STOP=TRUE, MIN_REL_PROGRESS=0.001) AS
SELECT
  * EXCEPT(geo_id)
FROM
  osm_clustering.grid_05km_vectors_300d
```
