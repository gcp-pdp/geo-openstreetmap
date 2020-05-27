#!/bin/sh
source ../query_templates.sh

CLASS=natural
LAYER=( 
        "4101:natural=spring"
        "4102:natural=glacier"
        "4111:natural=peak"
        "4112:natural=cliff"
        "4113:natural=volcano"
        "4121:natural=tree"
        "4131:natural=mine>mine-natural"
        "4131:historic=mine>mine-historic"
        "4131:landuse=mine>mine-landuse"
        "4131:survey_point=mine>mine-survey_point"
        "4131:industrial=mine>mine-industrial"
        "4132:natural=cave_entrance"
        "4141:natural=beach"
        "8300:natural=coastline"
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
