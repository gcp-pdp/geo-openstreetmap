CREATE OR REPLACE FUNCTION udfs.decodeGeoHash(geohash STRING)
  RETURNS STRUCT<latitude ARRAY<FLOAT64>, longitude ARRAY<FLOAT64>>
  LANGUAGE js
  OPTIONS (
    library=["gs://gcp-pdp-osm-dev-bq-udf/gis/geohash.js"]
  )
  AS
"""
    return decodeGeoHash(geohash);
""";
SELECT udfs.decodeGeoHash('0000');