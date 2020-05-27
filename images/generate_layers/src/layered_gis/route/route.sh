#!/bin/sh
source ../query_templates.sh

CLASS=route
LAYER=(
        "9001:route=bicycle"
        "9002:route=mtb"
        "9003:route=hiking"
        "9004:route=horse"
        "9005:route=nordic_walking"
        "9006:route=running"
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
