#!/bin/sh
source ../query_templates.sh

CLASS=barrier
LAYER=( 
        "5501:barrier=fence>fence-barrier"
        "5501:barrier=wood_fence>fence-wood_fence"
        "5501:barrier=wire_fence>fence-wire_fence"
        "5511:barrier=hedge"
        "5512:barrier=tree_row"
        "5521:barrier=wall"
        "5531:man_made=dyke"
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
