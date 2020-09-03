#!/bin/bash
PROCESSING_MODE="$1"

BQ_DATASET_TO_EXPORT_WITH_COLON=$(echo $BQ_DATASET_TO_EXPORT | sed 's/\./:/')

i=0
mode=""
for SQL in `find ../sql/ -type f -name '*.sql' | sort`; do
  echo $SQL
  if (($i > 0)); then
    mode="--append_table"
  else
    mode="--replace"
  fi

  cmd="cat $SQL | bq query\
  --project_id ${PROJECT_ID}\
  --nouse_legacy_sql\
  $mode\
  --range_partitioning 'layer_code,0,9999,1'\
  --clustering_fields 'layer_code,geometry'\
  --display_name $SQL\
  --destination_table '${BQ_DATASET_TO_EXPORT_WITH_COLON}.${PROCESSING_MODE}_layers'\
  --destination_schema ../schema/layers_schema.json >/dev/null"

  echo "$cmd"
  echo "$cmd" | bash

  ((i=i+1))
done
