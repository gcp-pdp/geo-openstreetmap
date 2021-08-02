BASE_32 = (
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q',
    'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
)


QUERY = """
SELECT
  grid.geohash AS geo_id,
  objects.term
FROM
  `gcp-pdp-osm-dev.geohash.objects_partitioned` AS objects,
  `gcp-pdp-osm-dev.geohash.level_7` AS grid
WHERE
  objects.geohash_partitioning = {}
  AND SUBSTR(grid.geohash, 1, 1) = "{}"
  AND ST_INTERSECTS(grid.geog, objects.geometry)
"""

CMD = 'bq query --batch --append_table --destination_table gcp-pdp-osm-dev:geohash.level_7_terms' \
      ' --nosynchronous_mode --nouse_legacy_sql \'{}\''

if __name__ == '__main__':
    for i, symbol in enumerate(BASE_32):
        query = QUERY.format(i, symbol).replace('\n', ' ')
        cmd = CMD.format(query)
        print(cmd)
