set -e

OGRCONFIG="$1"
SRC_FILE="$2"
DEST_FILE="$3"
OGR_TYPE="$4"

if [ "$OGR_TYPE" = "multipolygons" ]
then
    osm_fields="osm_id, osm_way_id"
else
    osm_fields="osm_id, NULL as osm_way_id"
fi
ogr2ogr \
    -skipfailures \
    -f GeoJSON \
    $DEST_FILE $SRC_FILE \
    --config OSM_CONFIG_FILE $OGRCONFIG \
    -dialect sqlite \
    -sql "select $osm_fields, AsGeoJSON(geometry) AS geometry, geometry from ${OGR_TYPE} where ST_IsValid(geometry) = 1" \
    --debug on \
    2> /dev/null