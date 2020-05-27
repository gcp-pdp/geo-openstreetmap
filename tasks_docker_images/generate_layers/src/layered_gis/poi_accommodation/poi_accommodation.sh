#!/bin/sh
source ../query_templates.sh

CLASS=poi_accommodation
LAYER=( 
        "2401:tourism=hotel"
        "2402:tourism=motel"
        "2403:tourism=bed_and_breakfast"
        "2404:tourism=guest_house"
        "2405:tourism=hostel"
        "2406:tourism=chalet"
        "2421:amenity=shelter"
        "2422:tourism=camp_site"
        "2423:tourism=alpine_hut"
        "2424:tourism=caravan_site"
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
