#!/bin/sh
source ../query_templates.sh

CLASS=poi_leisure
LAYER=( 
        "2201:amenity=theatre"
        "2202:amenity=nightclub"
        "2203:amenity=cinema"
        "2204:leisure=park"
        "2205:leisure=playground"
        "2206:leisure=dog_park"
        "2251:leisure=sports_centre"
        "2252:leisure=pitch"
        "2254:sport=tennis>tennis_court"
        "2255:leisure=golf_course"
        "2256:leisure=stadium"
        "2257:leisure=ice_rink"
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

CODE=2253
N=swimming_pool
F=swimming_pool
EXTRA_CONSTRAINTS="
  AND (
    EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'amenity' AND tags.value='swimming_pool')
      OR
    EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'leisure' AND tags.value='swimming_pool')
      OR
    EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'sport' AND tags.value='swimming')
      OR
    EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'leisure' AND tags.value='water_park')
  )"
common_query > "../../sql/$F.sql"
