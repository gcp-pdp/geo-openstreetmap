#!/bin/sh
source ../query_templates.sh

CLASS=traffic
LAYER=( 
        "5201:highway=traffic_signals"
        "5202:highway=mini_roundabout"
        "5203:highway=stop"
        "5204:highway=crossing>crossing-highway"
        "5204:railway=level_crossing>crossing-railway"
        "5205:highway=ford"
        "5206:highway=motorway_junction"
        "5207:highway=turning_circle"
        "5208:highway=speed_camera"
        "5209:highway=street_lamp"
        "5250:amenity=fuel"
        "5251:highway=services>service"
        "5270:amenity=bicycle_parking"
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

LAYER=( 
        "5261:parking=surface>parking_site"
        "5262:parking=multi-storey>parking_multistorey"
        "5263:parking=underground>parking_underground"
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
  AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'amenity' AND tags.value='parking')"
  common_query > "../../sql/$F.sql"
done


CODE=5260
N=parking
F=parking
EXTRA_CONSTRAINTS="
  AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'amenity' AND tags.value='parking')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'parking' AND tags.value='surface')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'parking' AND tags.value='multi-storey')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'parking' AND tags.value='underground')"
common_query > "../../sql/$F.sql"
