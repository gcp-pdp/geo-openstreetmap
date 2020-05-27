#!/bin/sh
source ../query_templates.sh

CLASS=power
LAYER=(
        "6411:source=nuclear>station_nuclear"
        "6412:source=solar>station_solar-solar"
        "6413:source=gas>station_fossil-gas"
        "6413:source=coal>station_fossil-coal"
        "6413:source=oil>station_fossil-oil"
        "6413:source=diesel>station_fossil-diesel"
        "6414:source=hydro>station_water-generator"
        "6415:source=wind>station_wind-generator"
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
  EXTRA_CONSTRAINTS="AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'generator:$K' AND tags.value='$V')"
  common_query > "../../sql/$F.sql"
done

LAYER=(
        "6204:power=pole>pole"
        "6401:power=tower>tower"
        "6412:power_source=photovoltaic>station_solar-photovoltaic"
        "6414:power_source=hydro>station_water-power"
        "6415:power_source=wind>station_wind-power"
        "6422:power=station>substation-station"
        "6422:power=sub_station>substation-sub_station"
        "6423:power=transformer>transformer"
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


CODE=6410
N=station
F=station
EXTRA_CONSTRAINTS="
  AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'power' AND tags.value='generator')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE
       (  tags.key = 'generator:source' AND tags.value = 'nuclear' ) 
    OR ( (tags.key = 'generator:source' AND tags.value = 'solar') OR (tags.key = 'power_source' AND tags.value = 'photovoltaic') ) 
    OR (  tags.key = 'generator:source' AND tags.value IN ('gas','coal','oil','diesel')  ) 
    OR ( (tags.key = 'generator:source' AND tags.value = 'hydro') OR (tags.key = 'power_source' AND tags.value = 'hydro') ) 
    OR ( (tags.key = 'generator:source' AND tags.value = 'wind') OR (tags.key = 'power_source' AND tags.value = 'wind') ) 
    OR ( (tags.key = 'power' AND tags.value = 'station') OR (tags.key = 'power' AND tags.value = 'sub_station') ) 
    OR (  tags.key = 'power' AND tags.value = 'transformer' ) 
  )"
common_query > "../../sql/$F.sql"
