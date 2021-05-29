BASE_32 = (
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q',
    'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
)

LEVEL_1 = (
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q',
    'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
)

QUERY = """
WITH lbcs_f_similarities AS (SELECT
  grid.geohash,
  MAX(udfs.cosine_similarity(tfidf.tfidf_vec, lbcs.tfidf_vec)) as max_similarity
FROM
  `gcp-pdp-osm-dev.geohash.level_7_vectors_tfidf` tfidf
JOIN `gcp-pdp-osm-dev.geohash.level_7` grid USING(geohash)
CROSS JOIN `gcp-pdp-osm-dev.lbcs.lbcs_tfidf_layers` lbcs
WHERE udfs.cosine_similarity(tfidf.tfidf_vec, lbcs.tfidf_vec) > 0
AND lbcs.dimension = "Function"
AND SUBSTR(grid.geohash, 1, 2) = "{0}"
GROUP BY grid.geohash)
SELECT
  grid.geohash,
  MAX(udfs.cosine_similarity(tfidf.tfidf_vec, lbcs.tfidf_vec)) as max_similarity_a,
  ANY_VALUE(lbcs_f.max_similarity) as max_similarity_f
FROM
  `gcp-pdp-osm-dev.geohash.level_7_vectors_tfidf` tfidf
JOIN `gcp-pdp-osm-dev.geohash.level_7` grid USING(geohash)
JOIN lbcs_f_similarities lbcs_f USING(geohash)
CROSS JOIN `gcp-pdp-osm-dev.lbcs.lbcs_tfidf_layers` lbcs
WHERE udfs.cosine_similarity(tfidf.tfidf_vec, lbcs.tfidf_vec) > 0
AND lbcs.dimension = "Activity"
AND SUBSTR(grid.geohash, 1, 2) = "{0}"
GROUP BY grid.geohash
"""

CMD = 'bq query --append_table --destination_table gcp-pdp-osm-dev:geohash.level_7_lbcs_similarities' \
      ' --nosynchronous_mode --nouse_legacy_sql \'{}\''

if __name__ == '__main__':
    for i in LEVEL_1:
        for j in BASE_32:
            geohash = i + j
            query = QUERY.format(geohash).replace('\n', ' ')
            cmd = CMD.format(query)
            print(cmd)
