#!/bin/bash
source ../query_templates.sh

CLASS=aeroway
LAYER=( 
        "6701:aeroway=runway"
        "6702:aeroway=taxiway"
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
