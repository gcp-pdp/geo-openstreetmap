BASE_32 = ('1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q',
    'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
)


QUERY = """
WITH data AS (
SELECT
  *
FROM `gcp-pdp-osm-dev.geohash.level_7_terms`
WHERE SUBSTR(geo_id, 1, 1) = "{}"
)
,features_matrix AS (
SELECT geo_id, CONCAT(gf.layer_code, "_", gf.layer_class, "_", gf.layer_name) as word
FROM `gcp-pdp-osm-dev.geofabrik.layers` gf
CROSS JOIN (SELECT geo_id FROM data GROUP BY geo_id)
ORDER BY geo_id, word
)
SELECT
  fm.geo_id as geohash, ARRAY_AGG(IFNULL(tf_idf.tfidf, 0.0) ORDER BY fm.word) as tfidf_vec
FROM features_matrix fm
LEFT JOIN `gcp-pdp-osm-dev.geohash.level_7_vectors_tfidf_raw` tf_idf ON tf_idf.term = fm.word AND tf_idf.geo_id = fm.geo_id
GROUP BY geohash
"""

CMD = 'bq query --batch --append_table --destination_table gcp-pdp-osm-dev:geohash.level_7_vectors_tfidf' \
      ' --nosynchronous_mode --nouse_legacy_sql \'{}\''

if __name__ == '__main__':
    for symbol in BASE_32:
        query = QUERY.format(symbol).replace('\n', ' ')
        cmd = CMD.format(query)
        print(cmd)
