#!/bin/sh
source ../query_templates.sh

CLASS=poi_money
LAYER=( 
        "2601:amenity=bank"
        "2602:amenity=atm"
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
