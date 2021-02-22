CREATE OR REPLACE FUNCTION udfs.euclidean_distance(a ARRAY<FLOAT64>, b ARRAY<FLOAT64>)
  RETURNS FLOAT64
  LANGUAGE js
  OPTIONS (
    library=["gs://gcp-pdp-osm-dev-bq-udf/metrics/metrics.js"]
  )
  AS
"""
    return euclideanDistances(a, b);
""";

SELECT EUCLIDEAN_DISTANCE([0., 0.], [1., 1.]);

CREATE OR REPLACE FUNCTION udfs.cosine_similarity(a ARRAY<FLOAT64>, b ARRAY<FLOAT64>)
  RETURNS FLOAT64
  LANGUAGE js
  OPTIONS (
    library=["gs://gcp-pdp-osm-dev-bq-udf/metrics/metrics.js"]
  )
  AS
"""
    return cosineSimilarity(a, b);
""";

SELECT COSINE_SIMILARITY([1., 4.], [1., 1.]);