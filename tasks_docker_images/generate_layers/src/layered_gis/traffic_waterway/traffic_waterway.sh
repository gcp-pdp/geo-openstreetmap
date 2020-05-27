#!/bin/sh
source ../query_templates.sh

CLASS=traffic
LAYER=( 
        "5301:leisure=slipway"
        "5302:leisure=marina"
        "5303:man_made=pier"
        "5311:waterway=dam"
        "5321:waterway=waterfall"
        "5331:waterway=lock_gate"
        "5332:waterway=weir"
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
  NAME_PREFIX=waterway_
  EXTRA_CONSTRAINTS="AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = '$K' AND tags.value='$V')"
  common_query > "../../sql/$F.sql"
done
