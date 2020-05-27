#!/bin/sh
source ../query_templates.sh

CLASS=poi_shopping
LAYER=( 
        "2501:shop=supermarket"
        "2502:shop=bakery"
        "2503:shop=kiosk"
        "2504:shop=mall"
        "2505:shop=department_store"
        "2511:shop=convenience"
        "2512:shop=clothes"
        "2513:shop=florist"
        "2514:shop=chemist"
        "2515:shop=books"
        "2516:shop=butcher"
        "2517:shop=shoes"
        "2518:shop=alcohol>beverages-alcohol"
        "2518:shop=beverages>beverages-beverages"
        "2519:shop=optician"
        "2520:shop=jewelry"
        "2521:shop=gift"
        "2522:shop=sports"
        "2523:shop=stationery"
        "2524:shop=outdoor"
        "2525:shop=mobile_phone"
        "2526:shop=toys"
        "2527:shop=newsagent"
        "2528:shop=greengrocer"
        "2529:shop=beauty"
        "2530:shop=video"
        "2541:shop=car"
        "2542:shop=bicycle"
        "2543:shop=doityourself>doityourself-doityourself"
        "2543:shop=hardware>doityourself-hardware"
        "2544:shop=furniture"
        "2546:shop=computer"
        "2547:shop=garden_centre"
        "2561:shop=hairdresser"
        "2562:shop=car_repair"
        "2563:amenity=car_rental"
        "2564:amenity=car_wash"
        "2565:amenity=car_sharing"
        "2566:amenity=bicycle_rental"
        "2567:shop=travel_agency"
        "2568:shop=laundry>laundry-laundry"
        "2568:shop=dry_cleaning>laundry-dry_cleaning"
        "2591:vending=cigarettes>vending_cigarette"
        "2592:vending=parking_tickets>vending_parking"
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

CODE=2590
N=vending_machine
F=vending_machine
EXTRA_CONSTRAINTS="
  AND EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'amenity' AND tags.value='vending_machine')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'vending' AND tags.value='cigarettes')
  AND NOT EXISTS(SELECT 1 FROM UNNEST(osm.all_tags) as tags WHERE tags.key = 'vending' AND tags.value='parking_tickets')"
common_query > "../../sql/$F.sql"
