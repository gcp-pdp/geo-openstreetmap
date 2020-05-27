#!/bin/sh
source ../query_templates.sh

CLASS=transport
LAYER=( 
        "5621:highway=bus_stop>bus_stop-highway"
        "5622:amenity=bus_station"
        "5641:amenity=taxi"
        "5652:aeroway=airfield>airfield-aeroway"
        "5652:military=airfield>airfield-military"
        "5655:aeroway=helipad"
        "5656:aeroway=apron"
        "5661:amenity=ferry_terminal"
        "5671:aerialway=station>aerialway_station"
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

CODE=5621
N=bus_stop
F=bus_stop-public_transport
#highway=bus_stop, or public_transport=stop_position + bus=yes
EXTRA_CONSTRAINTS="
  AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'public_transport' AND tags.value='stop_position')
  AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'bus' AND tags.value='yes')
  AND COALESCE(osm.id,osm.way_id) = COALESCE(f.osm_id,f.osm_way_id)"
common_query > "../../sql/$F.sql"

CODE=5651
N=airport
F=airport
#amenity=airport or aeroway=aerodrome unless type=airstrip
EXTRA_CONSTRAINTS="
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE (tags.key = 'type' AND tags.value='airstrip'))
  AND (
      EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'amenity' AND tags.value='airport')
      OR
      EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'aeroway' AND tags.value='aerodrome')
      )"
common_query > "../../sql/$F.sql"

CODE=5652
N=airfield
F=airfield-airstrip
#aeroway=airfield, military=airfield, aeroway=aeroway with type=airstrip
EXTRA_CONSTRAINTS="
  AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'aeroway' AND tags.value='aeroway')
  AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'type' AND tags.value='airstrip')"
common_query > "../../sql/$F.sql"
