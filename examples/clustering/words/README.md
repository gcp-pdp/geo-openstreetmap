## Import Glove vectors into BigQuery
Download word2vec (Glove)
```
cd ../data
wget http://nlp.stanford.edu/data/glove.6B.zip
unzip ./glove.6B.zip
rm ./glove.6B.zip
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