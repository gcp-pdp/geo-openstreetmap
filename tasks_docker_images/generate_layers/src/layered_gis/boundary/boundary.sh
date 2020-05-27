#!/bin/sh
source ../query_templates.sh

CLASS=boundary
LAYER=(
        "1101:admin_level=1>admin_level1"
        "1102:admin_level=2>national"
        "1103:admin_level=3>admin_level3"
        "1104:admin_level=4>admin_level4"
        "1105:admin_level=5>admin_level5"
        "1106:admin_level=6>admin_level6"
        "1107:admin_level=7>admin_level7"
        "1108:admin_level=8>admin_level8"
        "1109:admin_level=9>admin_level9"
        "1110:admin_level=10>admin_level10"
        "1111:admin_level=11>admin_level11"
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

  EXTRA_CONSTRAINTS="AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = '$K' AND tags.value='$V')
  AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'boundary' AND tags.value='administrative')"
  common_query > "../../sql/$F.sql"
done
