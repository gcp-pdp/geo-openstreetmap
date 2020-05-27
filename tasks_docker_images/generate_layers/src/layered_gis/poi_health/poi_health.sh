#!/bin/sh
source ../query_templates.sh

CLASS=poi_health
LAYER=( 
        "2101:amenity=pharmacy"
        "2110:amenity=hospital"
        "2120:amenity=doctors"
        "2121:amenity=dentist"
        "2129:amenity=veterinary"
)

for layer in "${LAYER[@]}"
do
  CODE="${layer%%:*}"
  KVF="${layer##*:}"
  K="${KVF%%=*}"
  VF="${KVF##*=}"
  V="${VF%%>*}"
  F="${VF##*>}"
  N="${F%%-*}"
  EXTRA_CONSTRAINTS="AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = '$K' AND tags.value='$V')"
  common_query > "../../sql/$F.sql"
done
