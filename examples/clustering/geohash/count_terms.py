QUERY = """
SELECT
  grid.geohash as geohash,
  objects.term
  FROM
  `gcp-pdp-osm-dev.geohash_v1.objects_partitioned` AS objects,
  `gcp-pdp-osm-dev.geohash.level_7_partitioned` as grid
WHERE ST_INTERSECTS(grid.geog, objects.geometry)
AND objects.geohash_l1_decimal = {0}
AND grid.geohash_l2_decimal >={0} * 32
AND grid.geohash_l2_decimal < ({0} + 1) * 32
"""

CMD = 'bq query --batch --append_table --destination_table gcp-pdp-osm-dev:geohash_v1.level_7_terms' \
      ' --{}_mode --nouse_legacy_sql \'{}\''

if __name__ == '__main__':
    for i in range(32):
        mode = 'nosynchronous'
        if i % 6 == 0:
            mode = 'synchronous'

        query = QUERY.format(i).replace('\n', ' ')
        cmd = CMD.format(mode, query)
        print(cmd)
