#!/bin/sh
source ../query_templates.sh

CLASS=traffic
LAYER=( 
        "5231:traffic_calming=hump"
        "5232:traffic_calming=bump"
        "5233:traffic_calming=table"
        "5234:traffic_calming=chicane"
        "5235:traffic_calming=cushion"
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
  NAME_PREFIX=calming_
  EXTRA_CONSTRAINTS="AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = '$K' AND tags.value='$V')"
  common_query > "../../sql/$NAME_PREFIX$F.sql"
done

#5230
CODE=5230
N=calming
F=calming
NAME_PREFIX=""
EXTRA_CONSTRAINTS="
  AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'traffic_calming')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'traffic_calming' AND tags.value='hump')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'traffic_calming' AND tags.value='bump')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'traffic_calming' AND tags.value='table')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'traffic_calming' AND tags.value='chicane')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'traffic_calming' AND tags.value='cushion')"
common_query > "../../sql/$F.sql"
