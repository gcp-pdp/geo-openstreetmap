#!/bin/sh
source ../query_templates.sh

CLASS=place
LAYER=(
        "1001:place=city"
        "1002:place=town"
        "1003:place=village"
        "1004:place=hamlet"
        "1010:place=suburb"
        "1020:place=island"
        "1030:place=farm"
        "1031:place=isolated_dwelling>dwelling"
        "1040:place=region"
        "1041:place=county"
        "1050:place=locality"
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

#1005
CODE=1005
N=national_capital
F=national_capital
EXTRA_CONSTRAINTS="
AND (
  (
    EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='city') AND
    EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'is_capital' AND tags.value='country')
  )
  OR
  (
    EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='city') AND
    EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'admin_level' AND tags.value = '2')
  )
  OR 
  (
    EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='city') AND
    EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'capital' AND tags.value='yes') AND
    NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'admin_level')
  )
)
"
common_query > "../../sql/$F.sql"

CODE=1099
N=named_place
F=named_place
EXTRA_CONSTRAINTS="
AND     EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'area'  AND tags.value='yes')
AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='city')
AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='town')
AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='village')
AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='hamlet')
AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='suburb')
AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='island')
AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='farm')
AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='isolated_dwelling')
AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='region')
AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='county')
AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='locality')
AND (
  (
    NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='city') AND
    NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'is_capital' AND tags.value='country')
  )
   OR
  (
    NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='city') AND
    NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'admin_level' AND tags.value = '2')
  )
   OR (
    NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'place' AND tags.value='city') AND
    NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'capital' AND tags.value='yes') AND
        EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) AS tags WHERE tags.key = 'admin_level')
   )
)
"
common_query > "../../sql/$F.sql"
