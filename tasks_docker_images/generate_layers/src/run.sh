#!/bin/bash
MODE="$1"
cd layered_gis

if [ $MODE == "planet" ]
then
cp query_templates_planet.sh query_templates.sh
else
cp query_templates_history.sh query_templates.sh
fi

echo "running ./00_generate_queries.sh"
./00_generate_queries.sh
echo "running ./01_append_table.sh"
./01_append_table.sh $MODE

