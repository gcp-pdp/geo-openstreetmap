# Cities

## List of Cities

In the `cities.csv` there is a list of cities from the [Globalization and World Cities Research Network](https://en.wikipedia.org/wiki/Globalization_and_World_Cities_Research_Network) wiki page.
Each city row contains lat/long and radius of the manually defined circle that approximately covers city infrastructure and agglomeration.

The `query.py` script can be used to transform CSV into the SQL with cities data.
In our example result of the query is saved into the `osm_cities.cities` table using the BigQuery console.

## OSM Objects within cities

Query to select all objects within city circle area.

```
SELECT
  cities.city_name,
  planet.*
FROM
  `bigquery-public-data.geo_openstreetmap.planet_layers` as planet, `gcp-pdp-osm-dev.osm_cities.cities` as cities
WHERE ST_DWITHIN(cities.center, planet.geometry, cities.radius)
```

Result is saved in `osm_cities.cities_objects` in order to reduce scanning overhead in next stages of analysis.

## Population grid within cities

### 1km resolution

Query to select [Worldpop](https://www.worldpop.org/) population grid cells within city circle area.
```
SELECT
  cities.city_name,
  grid.*
FROM `bigquery-public-data.worldpop.population_grid_1km` AS grid,
gcp-pdp-osm-dev.osm_cities.cities AS cities
WHERE last_updated = '2020-01-01'
AND ST_DWITHIN(cities.center, grid.geog, cities.radius)
```

Result is saved in `osm_cities.cities_population_grid_1km` in order to reduce scanning overhead in next stages of analysis.

### 0.5km resolution

Query to simply divide 1km resolution grid
```
WITH divided_grid AS (SELECT
  long1 + x*(long2 - long1)/2 as long1,
  lat1 + y*(lat2 - lat1)/2 as lat1,
  long1 + (x+1)*(long2 - long1)/2 as long2,
  lat1 + (y+1)*(lat2 - lat1)/2 as lat2,
  country_name,
  geo_id,
  population,
  alpha_3_code,
  last_updated
FROM (
WITH quadrants AS
(SELECT 0 as x, 0 as y UNION ALL
  SELECT 1, 0 UNION ALL
  SELECT 0, 1 UNION ALL
  SELECT 1, 1)
SELECT
  CAST(JSON_EXTRACT(ST_ASGEOJSON(geog), '$.coordinates[0][0][0]') AS FLOAT64) as long1,
  CAST(JSON_EXTRACT(ST_ASGEOJSON(geog), '$.coordinates[0][0][1]') AS FLOAT64) as lat1,
  CAST(JSON_EXTRACT(ST_ASGEOJSON(geog), '$.coordinates[0][2][0]') AS FLOAT64) as long2,
  CAST(JSON_EXTRACT(ST_ASGEOJSON(geog), '$.coordinates[0][2][1]') AS FLOAT64) as lat2,
  quadrants.x,
  quadrants.y,
  country_name,
  CONCAT(geo_id,x,y) as geo_id,
  population/4 as population,
  alpha_3_code,
  last_updated
FROM `osm_cities.cities_population_grid_1km`
CROSS JOIN quadrants
))
SELECT
  country_name,
  geo_id,
  population,
  (long1 + long2) / 2 as longitude_centroid,
  (lat1 + lat2) / 2  as latitude_centroid,
  alpha_3_code,
  ST_MAKEPOLYGON(ST_MAKELINE([
    ST_MAKELINE(ST_GEOGPOINT(long1, lat1), ST_GEOGPOINT(long1, lat2)),
    ST_MAKELINE(ST_GEOGPOINT(long1, lat2), ST_GEOGPOINT(long2, lat2)),
    ST_MAKELINE(ST_GEOGPOINT(long2, lat2), ST_GEOGPOINT(long2, lat1)),
    ST_MAKELINE(ST_GEOGPOINT(long2, lat1), ST_GEOGPOINT(long1, lat1))
  ])) as geog,
  last_updated
FROM divided_grid
```
Result is saved in `osm_cities.cities_population_grid_05km`.

### 0.25km resolution

Query to simply divide 0.5km resolution grid
```
WITH divided_grid AS (SELECT
  long1 + x*(long2 - long1)/2 as long1,
  lat1 + y*(lat2 - lat1)/2 as lat1,
  long1 + (x + 1)*(long2 - long1)/2 as long2,
  lat1 + (y + 1)*(lat2 - lat1)/2 as lat2,
  country_name,
  geo_id,
  population,
  alpha_3_code,
  last_updated
FROM (
WITH quadrants AS
(SELECT 0 as x, 0 as y UNION ALL
  SELECT 1, 0 UNION ALL
  SELECT 0, 1 UNION ALL
  SELECT 1, 1)
SELECT
  CAST(JSON_EXTRACT(ST_ASGEOJSON(geog), '$.coordinates[0][0][0]') AS FLOAT64) as long1,
  CAST(JSON_EXTRACT(ST_ASGEOJSON(geog), '$.coordinates[0][0][1]') AS FLOAT64) as lat1,
  CAST(JSON_EXTRACT(ST_ASGEOJSON(geog), '$.coordinates[0][2][0]') AS FLOAT64) as long2,
  CAST(JSON_EXTRACT(ST_ASGEOJSON(geog), '$.coordinates[0][2][1]') AS FLOAT64) as lat2,
  quadrants.x,
  quadrants.y,
  country_name,
  CONCAT(geo_id,x,y) as geo_id,
  population/4 as population,
  alpha_3_code,
  last_updated
FROM `osm_cities.cities_population_grid_05km`
CROSS JOIN quadrants
))
SELECT
  country_name,
  geo_id,
  population,
  (long1 + long2) / 2 as longitude_centroid,
  (lat1 + lat2) / 2  as latitude_centroid,
  alpha_3_code,
  ST_MAKEPOLYGON(ST_MAKELINE([
    ST_MAKELINE(ST_GEOGPOINT(long1, lat1), ST_GEOGPOINT(long1, lat2)),
    ST_MAKELINE(ST_GEOGPOINT(long1, lat2), ST_GEOGPOINT(long2, lat2)),
    ST_MAKELINE(ST_GEOGPOINT(long2, lat2), ST_GEOGPOINT(long2, lat1)),
    ST_MAKELINE(ST_GEOGPOINT(long2, lat1), ST_GEOGPOINT(long1, lat1))
  ])) as geog,
  last_updated
FROM divided_grid
```
Result is saved in `osm_cities.cities_population_grid_025km`.
