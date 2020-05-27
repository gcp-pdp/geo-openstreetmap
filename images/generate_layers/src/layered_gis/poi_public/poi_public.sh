#!/bin/sh
source ../query_templates.sh

CLASS=poi_public
LAYER=(
        "2001:amenity=police"
        "2002:amenity=fire_station"
        "2004:amenity=post_box"
        "2005:amenity=post_office"
        "2006:amenity=telephone"
        "2007:amenity=library"
        "2008:amenity=townhall>town_hall"
        "2009:amenity=courthouse"
        "2010:amenity=prison"
        "2011:amenity=embassy"
        "2012:amenity=community_centre"
        "2013:amenity=nursing_home"
        "2014:amenity=arts_centre"
        "2015:amenity=grave_yard>graveyard-amenity"
        "2015:landuse=cemetery>graveyard-landuse"
        "2016:amenity=marketplace"
        "2081:amenity=university"
        "2082:amenity=school"
        "2083:amenity=kindergarten"
        "2084:amenity=college"
        "2099:amenity=public_building"
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
        "2031:glass=yes>recycling_glass"
        "2032:paper=yes>recycling_paper"
        "2033:clothes=yes>recycling_clothes"
        "2034:metal=yes>recycling_metal"
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
  EXTRA_CONSTRAINTS="AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'recycling:$K' AND tags.value='$V')"
  common_query > "../../sql/$F.sql"
done

CODE=2030
N=recycling
F=recycling
EXTRA_CONSTRAINTS="
  AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'amenity' AND tags.value='recycling')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'recycling:glass' AND tags.value='yes')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'recycling:paper' AND tags.value='yes')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'recycling:clothes' AND tags.value='yes')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'recycling:scrap_metal' AND tags.value='yes')
"
common_query > "../../sql/$F.sql"
