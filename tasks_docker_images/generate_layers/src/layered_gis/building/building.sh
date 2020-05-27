#!/bin/sh
source ../query_templates.sh

CODE=1500
CLASS=building
K=building
V=building
N=building
F=building
EXTRA_CONSTRAINTS="AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = '$K')"
common_query > "../../sql/$F.sql"
