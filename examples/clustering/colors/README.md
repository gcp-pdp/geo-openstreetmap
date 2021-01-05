Download document with coloring scheme
```shell script
wget -P ../data/ https://planning-org-uploaded-media.s3.amazonaws.com/document/LBCS.pdf
```

Convert document sections to vectors
```shell script
python vectorize.py > colors.jsonl
```

Import colors.jsonl into BigQuery.
