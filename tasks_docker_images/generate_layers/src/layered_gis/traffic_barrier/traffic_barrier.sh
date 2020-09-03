#!/bin/sh
source ../query_templates.sh

CLASS=traffic
LAYER=( 
        "5211:barrier=gate"
        "5212:barrier=bollard"
        "5213:barrier=lift_gate"
        "5214:barrier=stile>stile-barrier"
        "5214:highway=stile>stile-highway"
        "5215:barrier=cycle_barrier>cycle"
        "5216:barrier=fence"
        "5217:barrier=toll_booth>toll"
        "5218:barrier=block"
        "5219:barrier=kissing_gate"
        "5220:barrier=cattle_grid"
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
  NAME_PREFIX=barrier_
  EXTRA_CONSTRAINTS="AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = '$K' AND tags.value='$V')"
  common_query > "../../sql/$NAME_PREFIX$F.sql"
done

CODE=5210
V=barrier
N=barrier
F=barrier
NAME_PREFIX=""
EXTRA_CONSTRAINTS="
  AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = '$K')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'barrier' AND tags.value='gate')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'barrier' AND tags.value='bollard')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'barrier' AND tags.value='lift_gate')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'barrier' AND tags.value='stile')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'highway' AND tags.value='stile')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'barrier' AND tags.value='cycle_barrier')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'barrier' AND tags.value='fence')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'barrier' AND tags.value='toll_booth')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'barrier' AND tags.value='block')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'barrier' AND tags.value='kissing_gate')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'barrier' AND tags.value='cattle_grid')"
common_query > "../../sql/$F.sql"
