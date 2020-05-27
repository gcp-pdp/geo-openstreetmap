#!/bin/bash
cd layered_gis
echo "running ./00_generate_queries.sh"
./00_generate_queries.sh
echo "running ./01_append_table.sh"
./01_append_table.sh

