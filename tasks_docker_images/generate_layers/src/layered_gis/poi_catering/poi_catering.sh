#!/bin/sh
source ../query_templates.sh

CLASS=poi_catering
LAYER=( 
        "2301:amenity=restaurant"
        "2302:amenity=fast_food"
        "2303:amenity=cafe"
        "2304:amenity=pub"
        "2305:amenity=bar"
        "2306:amenity=food_court"
        "2307:amenity=biergarten"
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
