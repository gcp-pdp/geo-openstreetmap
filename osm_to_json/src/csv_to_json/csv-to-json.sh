#!/usr/bin/env bash
set -e
CSV_FILES_PATH="$1"

for csv_file in $(ls -d ${CSV_FILES_PATH}*.geojson.csv);
do
    cat ${csv_file} \
    | perl csv_to_json/geojson-csv-to-json.pl \
    2> ${csv_file}.errors.jsonl > ${csv_file}.jsonl
done
