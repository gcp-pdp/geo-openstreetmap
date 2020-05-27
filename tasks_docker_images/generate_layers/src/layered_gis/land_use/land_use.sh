#!/bin/sh
source ../query_templates.sh

CLASS=land_use
LAYER=( 
        "7201:landuse=forest>forest-landuse"
        "7201:natural=wood>forest-natural"
        "7202:leisure=park>park-park"
        "7202:leisure=common>park-common"
        "7203:landuse=residential"
        "7204:landuse=industrial"
        "7206:amenity=grave_yard>cemetery-amenity"
        "7206:landuse=cemetery>cemetery-landuse"
        "7207:landuse=allotments"
        "7208:landuse=meadow"
        "7209:landuse=commercial"
        "7210:leisure=nature_reserve"
        "7211:leisure=recreation_ground>recreation_ground-leisure"
        "7211:landuse=recreation_ground>recreation_ground-landuse"
        "7212:landuse=retail"
        "7213:landuse=military"
        "7214:landuse=quarry"
        "7215:landuse=orchard"
        "7216:landuse=vineyard"
        "7217:landuse=scrub"
        "7218:landuse=grass"
        "7219:landuse=heath"
        "7220:boundary=national_park"
        "7221:landuse=basin"
        "7222:landuse=village_green"
        "7223:landuse=plant_nursery"
        "7224:landuse=brownfield"
        "7225:landuse=greenfield"
        "7226:landuse=construction"
        "7227:landuse=railway"
        "7228:landuse=farmland"
        "7229:landuse=farmyard"

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
