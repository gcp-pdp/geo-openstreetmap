## Import Glove vectors into BigQuery
Download word2vec (Glove)
```
cd ./data
wget http://nlp.stanford.edu/data/glove.6B.zip
unzip ./glove.6B.zip
rm ./glove.6B.zip
cd ../
```

Convert word2vec to JSONL format
```
cat ./data/glove.6B.300d.txt | python3 w2v_to_jsonl.py > ./data/glove.6B.300d.jsonl
```

Upload result to GCS:
```
gsutil cp ./data/glove.6B.300d.jsonl gs://gcp-pdp-osm-dev-bq-import/glove/
```

Import into BQ:
```
bq load \
 --source_format=NEWLINE_DELIMITED_JSON \
 gcp-pdp-osm-dev:osm_clustering.w2v_glove_6B_300d \
 gs://gcp-pdp-osm-dev-bq-import/glove.6B.300d.jsonl \
 "$(python3 w2v_generate_schema.py 300)"
```

## Prepare grid

```sql
SELECT
  *
FROM `bigquery-public-data.worldpop.population_grid_1km` AS grid,
gcp-pdp-osm-dev.osm_clustering.cities_circles AS cities
WHERE last_updated = '2020-01-01'
AND ST_DWITHIN(cities.center, grid.geog, cities.radius)
```

## Select objects
```sql
SELECT
  cities.city_name,
  planet.*
FROM
  `bigquery-public-data.geo_openstreetmap.planet_layers` as planet, `gcp-pdp-osm-dev.osm_clustering.cities_circles` as cities
WHERE ST_DWITHIN(cities.center, planet.geometry, cities.radius)
```

## Build grid vectors
```sql
WITH objects_with_vectors AS (SELECT osm_id, geometry, w2v.*
FROM `gcp-pdp-osm-dev.osm_clustering.cities_c_objects` as objects
JOIN UNNEST(SPLIT(CONCAT(layer_class, "_", layer_name), "_")) as term
LEFT JOIN `gcp-pdp-osm-dev.osm_clustering.w2v_glove_6B_300d` AS w2v ON term = w2v.word)
SELECT
  grid.geo_id,
  SUM(objects.f1)/COUNT(objects.f1) as f1,
  SUM(objects.f2)/COUNT(objects.f2) as f2,
  SUM(objects.f3)/COUNT(objects.f3) as f3,
  SUM(objects.f4)/COUNT(objects.f4) as f4,
  SUM(objects.f5)/COUNT(objects.f5) as f5,
  SUM(objects.f6)/COUNT(objects.f6) as f6,
  SUM(objects.f7)/COUNT(objects.f7) as f7,
  SUM(objects.f8)/COUNT(objects.f8) as f8,
  SUM(objects.f9)/COUNT(objects.f9) as f9,
  SUM(objects.f10)/COUNT(objects.f10) as f10,
  SUM(objects.f11)/COUNT(objects.f11) as f11,
  SUM(objects.f12)/COUNT(objects.f12) as f12,
  SUM(objects.f13)/COUNT(objects.f13) as f13,
  SUM(objects.f14)/COUNT(objects.f14) as f14,
  SUM(objects.f15)/COUNT(objects.f15) as f15,
  SUM(objects.f16)/COUNT(objects.f16) as f16,
  SUM(objects.f17)/COUNT(objects.f17) as f17,
  SUM(objects.f18)/COUNT(objects.f18) as f18,
  SUM(objects.f19)/COUNT(objects.f19) as f19,
  SUM(objects.f20)/COUNT(objects.f20) as f20,
  SUM(objects.f21)/COUNT(objects.f21) as f21,
  SUM(objects.f22)/COUNT(objects.f22) as f22,
  SUM(objects.f23)/COUNT(objects.f23) as f23,
  SUM(objects.f24)/COUNT(objects.f24) as f24,
  SUM(objects.f25)/COUNT(objects.f25) as f25,
  SUM(objects.f26)/COUNT(objects.f26) as f26,
  SUM(objects.f27)/COUNT(objects.f27) as f27,
  SUM(objects.f28)/COUNT(objects.f28) as f28,
  SUM(objects.f29)/COUNT(objects.f29) as f29,
  SUM(objects.f30)/COUNT(objects.f30) as f30,
  SUM(objects.f31)/COUNT(objects.f31) as f31,
  SUM(objects.f32)/COUNT(objects.f32) as f32,
  SUM(objects.f33)/COUNT(objects.f33) as f33,
  SUM(objects.f34)/COUNT(objects.f34) as f34,
  SUM(objects.f35)/COUNT(objects.f35) as f35,
  SUM(objects.f36)/COUNT(objects.f36) as f36,
  SUM(objects.f37)/COUNT(objects.f37) as f37,
  SUM(objects.f38)/COUNT(objects.f38) as f38,
  SUM(objects.f39)/COUNT(objects.f39) as f39,
  SUM(objects.f40)/COUNT(objects.f40) as f40,
  SUM(objects.f41)/COUNT(objects.f41) as f41,
  SUM(objects.f42)/COUNT(objects.f42) as f42,
  SUM(objects.f43)/COUNT(objects.f43) as f43,
  SUM(objects.f44)/COUNT(objects.f44) as f44,
  SUM(objects.f45)/COUNT(objects.f45) as f45,
  SUM(objects.f46)/COUNT(objects.f46) as f46,
  SUM(objects.f47)/COUNT(objects.f47) as f47,
  SUM(objects.f48)/COUNT(objects.f48) as f48,
  SUM(objects.f49)/COUNT(objects.f49) as f49,
  SUM(objects.f50)/COUNT(objects.f50) as f50,
  SUM(objects.f51)/COUNT(objects.f51) as f51,
  SUM(objects.f52)/COUNT(objects.f52) as f52,
  SUM(objects.f53)/COUNT(objects.f53) as f53,
  SUM(objects.f54)/COUNT(objects.f54) as f54,
  SUM(objects.f55)/COUNT(objects.f55) as f55,
  SUM(objects.f56)/COUNT(objects.f56) as f56,
  SUM(objects.f57)/COUNT(objects.f57) as f57,
  SUM(objects.f58)/COUNT(objects.f58) as f58,
  SUM(objects.f59)/COUNT(objects.f59) as f59,
  SUM(objects.f60)/COUNT(objects.f60) as f60,
  SUM(objects.f61)/COUNT(objects.f61) as f61,
  SUM(objects.f62)/COUNT(objects.f62) as f62,
  SUM(objects.f63)/COUNT(objects.f63) as f63,
  SUM(objects.f64)/COUNT(objects.f64) as f64,
  SUM(objects.f65)/COUNT(objects.f65) as f65,
  SUM(objects.f66)/COUNT(objects.f66) as f66,
  SUM(objects.f67)/COUNT(objects.f67) as f67,
  SUM(objects.f68)/COUNT(objects.f68) as f68,
  SUM(objects.f69)/COUNT(objects.f69) as f69,
  SUM(objects.f70)/COUNT(objects.f70) as f70,
  SUM(objects.f71)/COUNT(objects.f71) as f71,
  SUM(objects.f72)/COUNT(objects.f72) as f72,
  SUM(objects.f73)/COUNT(objects.f73) as f73,
  SUM(objects.f74)/COUNT(objects.f74) as f74,
  SUM(objects.f75)/COUNT(objects.f75) as f75,
  SUM(objects.f76)/COUNT(objects.f76) as f76,
  SUM(objects.f77)/COUNT(objects.f77) as f77,
  SUM(objects.f78)/COUNT(objects.f78) as f78,
  SUM(objects.f79)/COUNT(objects.f79) as f79,
  SUM(objects.f80)/COUNT(objects.f80) as f80,
  SUM(objects.f81)/COUNT(objects.f81) as f81,
  SUM(objects.f82)/COUNT(objects.f82) as f82,
  SUM(objects.f83)/COUNT(objects.f83) as f83,
  SUM(objects.f84)/COUNT(objects.f84) as f84,
  SUM(objects.f85)/COUNT(objects.f85) as f85,
  SUM(objects.f86)/COUNT(objects.f86) as f86,
  SUM(objects.f87)/COUNT(objects.f87) as f87,
  SUM(objects.f88)/COUNT(objects.f88) as f88,
  SUM(objects.f89)/COUNT(objects.f89) as f89,
  SUM(objects.f90)/COUNT(objects.f90) as f90,
  SUM(objects.f91)/COUNT(objects.f91) as f91,
  SUM(objects.f92)/COUNT(objects.f92) as f92,
  SUM(objects.f93)/COUNT(objects.f93) as f93,
  SUM(objects.f94)/COUNT(objects.f94) as f94,
  SUM(objects.f95)/COUNT(objects.f95) as f95,
  SUM(objects.f96)/COUNT(objects.f96) as f96,
  SUM(objects.f97)/COUNT(objects.f97) as f97,
  SUM(objects.f98)/COUNT(objects.f98) as f98,
  SUM(objects.f99)/COUNT(objects.f99) as f99,
  SUM(objects.f100)/COUNT(objects.f100) as f100,
  SUM(objects.f101)/COUNT(objects.f101) as f101,
  SUM(objects.f102)/COUNT(objects.f102) as f102,
  SUM(objects.f103)/COUNT(objects.f103) as f103,
  SUM(objects.f104)/COUNT(objects.f104) as f104,
  SUM(objects.f105)/COUNT(objects.f105) as f105,
  SUM(objects.f106)/COUNT(objects.f106) as f106,
  SUM(objects.f107)/COUNT(objects.f107) as f107,
  SUM(objects.f108)/COUNT(objects.f108) as f108,
  SUM(objects.f109)/COUNT(objects.f109) as f109,
  SUM(objects.f110)/COUNT(objects.f110) as f110,
  SUM(objects.f111)/COUNT(objects.f111) as f111,
  SUM(objects.f112)/COUNT(objects.f112) as f112,
  SUM(objects.f113)/COUNT(objects.f113) as f113,
  SUM(objects.f114)/COUNT(objects.f114) as f114,
  SUM(objects.f115)/COUNT(objects.f115) as f115,
  SUM(objects.f116)/COUNT(objects.f116) as f116,
  SUM(objects.f117)/COUNT(objects.f117) as f117,
  SUM(objects.f118)/COUNT(objects.f118) as f118,
  SUM(objects.f119)/COUNT(objects.f119) as f119,
  SUM(objects.f120)/COUNT(objects.f120) as f120,
  SUM(objects.f121)/COUNT(objects.f121) as f121,
  SUM(objects.f122)/COUNT(objects.f122) as f122,
  SUM(objects.f123)/COUNT(objects.f123) as f123,
  SUM(objects.f124)/COUNT(objects.f124) as f124,
  SUM(objects.f125)/COUNT(objects.f125) as f125,
  SUM(objects.f126)/COUNT(objects.f126) as f126,
  SUM(objects.f127)/COUNT(objects.f127) as f127,
  SUM(objects.f128)/COUNT(objects.f128) as f128,
  SUM(objects.f129)/COUNT(objects.f129) as f129,
  SUM(objects.f130)/COUNT(objects.f130) as f130,
  SUM(objects.f131)/COUNT(objects.f131) as f131,
  SUM(objects.f132)/COUNT(objects.f132) as f132,
  SUM(objects.f133)/COUNT(objects.f133) as f133,
  SUM(objects.f134)/COUNT(objects.f134) as f134,
  SUM(objects.f135)/COUNT(objects.f135) as f135,
  SUM(objects.f136)/COUNT(objects.f136) as f136,
  SUM(objects.f137)/COUNT(objects.f137) as f137,
  SUM(objects.f138)/COUNT(objects.f138) as f138,
  SUM(objects.f139)/COUNT(objects.f139) as f139,
  SUM(objects.f140)/COUNT(objects.f140) as f140,
  SUM(objects.f141)/COUNT(objects.f141) as f141,
  SUM(objects.f142)/COUNT(objects.f142) as f142,
  SUM(objects.f143)/COUNT(objects.f143) as f143,
  SUM(objects.f144)/COUNT(objects.f144) as f144,
  SUM(objects.f145)/COUNT(objects.f145) as f145,
  SUM(objects.f146)/COUNT(objects.f146) as f146,
  SUM(objects.f147)/COUNT(objects.f147) as f147,
  SUM(objects.f148)/COUNT(objects.f148) as f148,
  SUM(objects.f149)/COUNT(objects.f149) as f149,
  SUM(objects.f150)/COUNT(objects.f150) as f150,
  SUM(objects.f151)/COUNT(objects.f151) as f151,
  SUM(objects.f152)/COUNT(objects.f152) as f152,
  SUM(objects.f153)/COUNT(objects.f153) as f153,
  SUM(objects.f154)/COUNT(objects.f154) as f154,
  SUM(objects.f155)/COUNT(objects.f155) as f155,
  SUM(objects.f156)/COUNT(objects.f156) as f156,
  SUM(objects.f157)/COUNT(objects.f157) as f157,
  SUM(objects.f158)/COUNT(objects.f158) as f158,
  SUM(objects.f159)/COUNT(objects.f159) as f159,
  SUM(objects.f160)/COUNT(objects.f160) as f160,
  SUM(objects.f161)/COUNT(objects.f161) as f161,
  SUM(objects.f162)/COUNT(objects.f162) as f162,
  SUM(objects.f163)/COUNT(objects.f163) as f163,
  SUM(objects.f164)/COUNT(objects.f164) as f164,
  SUM(objects.f165)/COUNT(objects.f165) as f165,
  SUM(objects.f166)/COUNT(objects.f166) as f166,
  SUM(objects.f167)/COUNT(objects.f167) as f167,
  SUM(objects.f168)/COUNT(objects.f168) as f168,
  SUM(objects.f169)/COUNT(objects.f169) as f169,
  SUM(objects.f170)/COUNT(objects.f170) as f170,
  SUM(objects.f171)/COUNT(objects.f171) as f171,
  SUM(objects.f172)/COUNT(objects.f172) as f172,
  SUM(objects.f173)/COUNT(objects.f173) as f173,
  SUM(objects.f174)/COUNT(objects.f174) as f174,
  SUM(objects.f175)/COUNT(objects.f175) as f175,
  SUM(objects.f176)/COUNT(objects.f176) as f176,
  SUM(objects.f177)/COUNT(objects.f177) as f177,
  SUM(objects.f178)/COUNT(objects.f178) as f178,
  SUM(objects.f179)/COUNT(objects.f179) as f179,
  SUM(objects.f180)/COUNT(objects.f180) as f180,
  SUM(objects.f181)/COUNT(objects.f181) as f181,
  SUM(objects.f182)/COUNT(objects.f182) as f182,
  SUM(objects.f183)/COUNT(objects.f183) as f183,
  SUM(objects.f184)/COUNT(objects.f184) as f184,
  SUM(objects.f185)/COUNT(objects.f185) as f185,
  SUM(objects.f186)/COUNT(objects.f186) as f186,
  SUM(objects.f187)/COUNT(objects.f187) as f187,
  SUM(objects.f188)/COUNT(objects.f188) as f188,
  SUM(objects.f189)/COUNT(objects.f189) as f189,
  SUM(objects.f190)/COUNT(objects.f190) as f190,
  SUM(objects.f191)/COUNT(objects.f191) as f191,
  SUM(objects.f192)/COUNT(objects.f192) as f192,
  SUM(objects.f193)/COUNT(objects.f193) as f193,
  SUM(objects.f194)/COUNT(objects.f194) as f194,
  SUM(objects.f195)/COUNT(objects.f195) as f195,
  SUM(objects.f196)/COUNT(objects.f196) as f196,
  SUM(objects.f197)/COUNT(objects.f197) as f197,
  SUM(objects.f198)/COUNT(objects.f198) as f198,
  SUM(objects.f199)/COUNT(objects.f199) as f199,
  SUM(objects.f200)/COUNT(objects.f200) as f200,
  SUM(objects.f201)/COUNT(objects.f201) as f201,
  SUM(objects.f202)/COUNT(objects.f202) as f202,
  SUM(objects.f203)/COUNT(objects.f203) as f203,
  SUM(objects.f204)/COUNT(objects.f204) as f204,
  SUM(objects.f205)/COUNT(objects.f205) as f205,
  SUM(objects.f206)/COUNT(objects.f206) as f206,
  SUM(objects.f207)/COUNT(objects.f207) as f207,
  SUM(objects.f208)/COUNT(objects.f208) as f208,
  SUM(objects.f209)/COUNT(objects.f209) as f209,
  SUM(objects.f210)/COUNT(objects.f210) as f210,
  SUM(objects.f211)/COUNT(objects.f211) as f211,
  SUM(objects.f212)/COUNT(objects.f212) as f212,
  SUM(objects.f213)/COUNT(objects.f213) as f213,
  SUM(objects.f214)/COUNT(objects.f214) as f214,
  SUM(objects.f215)/COUNT(objects.f215) as f215,
  SUM(objects.f216)/COUNT(objects.f216) as f216,
  SUM(objects.f217)/COUNT(objects.f217) as f217,
  SUM(objects.f218)/COUNT(objects.f218) as f218,
  SUM(objects.f219)/COUNT(objects.f219) as f219,
  SUM(objects.f220)/COUNT(objects.f220) as f220,
  SUM(objects.f221)/COUNT(objects.f221) as f221,
  SUM(objects.f222)/COUNT(objects.f222) as f222,
  SUM(objects.f223)/COUNT(objects.f223) as f223,
  SUM(objects.f224)/COUNT(objects.f224) as f224,
  SUM(objects.f225)/COUNT(objects.f225) as f225,
  SUM(objects.f226)/COUNT(objects.f226) as f226,
  SUM(objects.f227)/COUNT(objects.f227) as f227,
  SUM(objects.f228)/COUNT(objects.f228) as f228,
  SUM(objects.f229)/COUNT(objects.f229) as f229,
  SUM(objects.f230)/COUNT(objects.f230) as f230,
  SUM(objects.f231)/COUNT(objects.f231) as f231,
  SUM(objects.f232)/COUNT(objects.f232) as f232,
  SUM(objects.f233)/COUNT(objects.f233) as f233,
  SUM(objects.f234)/COUNT(objects.f234) as f234,
  SUM(objects.f235)/COUNT(objects.f235) as f235,
  SUM(objects.f236)/COUNT(objects.f236) as f236,
  SUM(objects.f237)/COUNT(objects.f237) as f237,
  SUM(objects.f238)/COUNT(objects.f238) as f238,
  SUM(objects.f239)/COUNT(objects.f239) as f239,
  SUM(objects.f240)/COUNT(objects.f240) as f240,
  SUM(objects.f241)/COUNT(objects.f241) as f241,
  SUM(objects.f242)/COUNT(objects.f242) as f242,
  SUM(objects.f243)/COUNT(objects.f243) as f243,
  SUM(objects.f244)/COUNT(objects.f244) as f244,
  SUM(objects.f245)/COUNT(objects.f245) as f245,
  SUM(objects.f246)/COUNT(objects.f246) as f246,
  SUM(objects.f247)/COUNT(objects.f247) as f247,
  SUM(objects.f248)/COUNT(objects.f248) as f248,
  SUM(objects.f249)/COUNT(objects.f249) as f249,
  SUM(objects.f250)/COUNT(objects.f250) as f250,
  SUM(objects.f251)/COUNT(objects.f251) as f251,
  SUM(objects.f252)/COUNT(objects.f252) as f252,
  SUM(objects.f253)/COUNT(objects.f253) as f253,
  SUM(objects.f254)/COUNT(objects.f254) as f254,
  SUM(objects.f255)/COUNT(objects.f255) as f255,
  SUM(objects.f256)/COUNT(objects.f256) as f256,
  SUM(objects.f257)/COUNT(objects.f257) as f257,
  SUM(objects.f258)/COUNT(objects.f258) as f258,
  SUM(objects.f259)/COUNT(objects.f259) as f259,
  SUM(objects.f260)/COUNT(objects.f260) as f260,
  SUM(objects.f261)/COUNT(objects.f261) as f261,
  SUM(objects.f262)/COUNT(objects.f262) as f262,
  SUM(objects.f263)/COUNT(objects.f263) as f263,
  SUM(objects.f264)/COUNT(objects.f264) as f264,
  SUM(objects.f265)/COUNT(objects.f265) as f265,
  SUM(objects.f266)/COUNT(objects.f266) as f266,
  SUM(objects.f267)/COUNT(objects.f267) as f267,
  SUM(objects.f268)/COUNT(objects.f268) as f268,
  SUM(objects.f269)/COUNT(objects.f269) as f269,
  SUM(objects.f270)/COUNT(objects.f270) as f270,
  SUM(objects.f271)/COUNT(objects.f271) as f271,
  SUM(objects.f272)/COUNT(objects.f272) as f272,
  SUM(objects.f273)/COUNT(objects.f273) as f273,
  SUM(objects.f274)/COUNT(objects.f274) as f274,
  SUM(objects.f275)/COUNT(objects.f275) as f275,
  SUM(objects.f276)/COUNT(objects.f276) as f276,
  SUM(objects.f277)/COUNT(objects.f277) as f277,
  SUM(objects.f278)/COUNT(objects.f278) as f278,
  SUM(objects.f279)/COUNT(objects.f279) as f279,
  SUM(objects.f280)/COUNT(objects.f280) as f280,
  SUM(objects.f281)/COUNT(objects.f281) as f281,
  SUM(objects.f282)/COUNT(objects.f282) as f282,
  SUM(objects.f283)/COUNT(objects.f283) as f283,
  SUM(objects.f284)/COUNT(objects.f284) as f284,
  SUM(objects.f285)/COUNT(objects.f285) as f285,
  SUM(objects.f286)/COUNT(objects.f286) as f286,
  SUM(objects.f287)/COUNT(objects.f287) as f287,
  SUM(objects.f288)/COUNT(objects.f288) as f288,
  SUM(objects.f289)/COUNT(objects.f289) as f289,
  SUM(objects.f290)/COUNT(objects.f290) as f290,
  SUM(objects.f291)/COUNT(objects.f291) as f291,
  SUM(objects.f292)/COUNT(objects.f292) as f292,
  SUM(objects.f293)/COUNT(objects.f293) as f293,
  SUM(objects.f294)/COUNT(objects.f294) as f294,
  SUM(objects.f295)/COUNT(objects.f295) as f295,
  SUM(objects.f296)/COUNT(objects.f296) as f296,
  SUM(objects.f297)/COUNT(objects.f297) as f297,
  SUM(objects.f298)/COUNT(objects.f298) as f298,
  SUM(objects.f299)/COUNT(objects.f299) as f299,
  SUM(objects.f300)/COUNT(objects.f300) as f300
FROM
  objects_with_vectors AS objects,
  `gcp-pdp-osm-dev.osm_clustering.cities_c_population_grid_05km` as grid
WHERE ST_INTERSECTS(grid.geog, objects.geometry)
GROUP BY grid.geo_id
```

## Create model

```sql
CREATE OR REPLACE MODEL
  osm_clustering.grid_05km_300d_clusters_10 OPTIONS(model_type='kmeans', num_clusters=10, max_iterations=50, EARLY_STOP=TRUE, MIN_REL_PROGRESS=0.001) AS
SELECT
  * EXCEPT(geo_id)
FROM
  osm_clustering.grid_05km_vectors_300d
```

## Run city analysis

```sql
SELECT
  grid.geog, CENTROID_ID, cs.color
FROM
  ML.PREDICT( MODEL osm_clustering.grid_05km_300d_clusters_10,
    (
    SELECT
      *
    FROM
      osm_clustering.grid_05km_vectors_300d
)) as clusters
JOIN osm_clustering.cities_c_population_grid_05km AS grid ON grid.geo_id = clusters.geo_id
JOIN osm_clustering.color_scale_clusters_10 cs ON cs.cluster = CENTROID_ID
WHERE grid.city_name = "Kyiv"
```

## Extract centroids

```sql
SELECT centroid_id, feature, numerical_value
FROM ML.CENTROIDS(MODEL `gcp-pdp-osm-dev.osm_clustering.grid_1km_50d_clusters_10`)
```

```sql
CALL fhoffa.x.pivot(
  'gcp-pdp-osm-dev.osm_clustering.grid_1km_300d_clusters_10_centroids' # source table
  , 'gcp-pdp-osm-dev.osm_clustering.grid_1km_300d_clusters_10_centroids_trans' # destination table
  , ['centroid_id'] # row_ids
  , 'feature' # pivot_col_name
  , 'numerical_value' # pivot_col_value
  , 301 # max_columns
  , 'ANY_VALUE' # aggregation
  , '' # optional_limit
);
```

## Cluster description

```sql
SELECT a.word, b.centroid_id,
SAFE_DIVIDE((a.f1*b.e_f1 + a.f2*b.e_f2 + a.f3*b.e_f3 + a.f4*b.e_f4 + a.f5*b.e_f5 + a.f6*b.e_f6 + a.f7*b.e_f7 + a.f8*b.e_f8 + a.f9*b.e_f9 + a.f10*b.e_f10 + a.f11*b.e_f11 + a.f12*b.e_f12 + a.f13*b.e_f13 + a.f14*b.e_f14 + a.f15*b.e_f15 + a.f16*b.e_f16 + a.f17*b.e_f17 + a.f18*b.e_f18 + a.f19*b.e_f19 + a.f20*b.e_f20 + a.f21*b.e_f21 + a.f22*b.e_f22 + a.f23*b.e_f23 + a.f24*b.e_f24 + a.f25*b.e_f25 + a.f26*b.e_f26 + a.f27*b.e_f27 + a.f28*b.e_f28 + a.f29*b.e_f29 + a.f30*b.e_f30 + a.f31*b.e_f31 + a.f32*b.e_f32 + a.f33*b.e_f33 + a.f34*b.e_f34 + a.f35*b.e_f35 + a.f36*b.e_f36 + a.f37*b.e_f37 + a.f38*b.e_f38 + a.f39*b.e_f39 + a.f40*b.e_f40 + a.f41*b.e_f41 + a.f42*b.e_f42 + a.f43*b.e_f43 + a.f44*b.e_f44 + a.f45*b.e_f45 + a.f46*b.e_f46 + a.f47*b.e_f47 + a.f48*b.e_f48 + a.f49*b.e_f49 + a.f50*b.e_f50 + a.f51*b.e_f51 + a.f52*b.e_f52 + a.f53*b.e_f53 + a.f54*b.e_f54 + a.f55*b.e_f55 + a.f56*b.e_f56 + a.f57*b.e_f57 + a.f58*b.e_f58 + a.f59*b.e_f59 + a.f60*b.e_f60 + a.f61*b.e_f61 + a.f62*b.e_f62 + a.f63*b.e_f63 + a.f64*b.e_f64 + a.f65*b.e_f65 + a.f66*b.e_f66 + a.f67*b.e_f67 + a.f68*b.e_f68 + a.f69*b.e_f69 + a.f70*b.e_f70 + a.f71*b.e_f71 + a.f72*b.e_f72 + a.f73*b.e_f73 + a.f74*b.e_f74 + a.f75*b.e_f75 + a.f76*b.e_f76 + a.f77*b.e_f77 + a.f78*b.e_f78 + a.f79*b.e_f79 + a.f80*b.e_f80 + a.f81*b.e_f81 + a.f82*b.e_f82 + a.f83*b.e_f83 + a.f84*b.e_f84 + a.f85*b.e_f85 + a.f86*b.e_f86 + a.f87*b.e_f87 + a.f88*b.e_f88 + a.f89*b.e_f89 + a.f90*b.e_f90 + a.f91*b.e_f91 + a.f92*b.e_f92 + a.f93*b.e_f93 + a.f94*b.e_f94 + a.f95*b.e_f95 + a.f96*b.e_f96 + a.f97*b.e_f97 + a.f98*b.e_f98 + a.f99*b.e_f99 + a.f100*b.e_f100 + a.f101*b.e_f101 + a.f102*b.e_f102 + a.f103*b.e_f103 + a.f104*b.e_f104 + a.f105*b.e_f105 + a.f106*b.e_f106 + a.f107*b.e_f107 + a.f108*b.e_f108 + a.f109*b.e_f109 + a.f110*b.e_f110 + a.f111*b.e_f111 + a.f112*b.e_f112 + a.f113*b.e_f113 + a.f114*b.e_f114 + a.f115*b.e_f115 + a.f116*b.e_f116 + a.f117*b.e_f117 + a.f118*b.e_f118 + a.f119*b.e_f119 + a.f120*b.e_f120 + a.f121*b.e_f121 + a.f122*b.e_f122 + a.f123*b.e_f123 + a.f124*b.e_f124 + a.f125*b.e_f125 + a.f126*b.e_f126 + a.f127*b.e_f127 + a.f128*b.e_f128 + a.f129*b.e_f129 + a.f130*b.e_f130 + a.f131*b.e_f131 + a.f132*b.e_f132 + a.f133*b.e_f133 + a.f134*b.e_f134 + a.f135*b.e_f135 + a.f136*b.e_f136 + a.f137*b.e_f137 + a.f138*b.e_f138 + a.f139*b.e_f139 + a.f140*b.e_f140 + a.f141*b.e_f141 + a.f142*b.e_f142 + a.f143*b.e_f143 + a.f144*b.e_f144 + a.f145*b.e_f145 + a.f146*b.e_f146 + a.f147*b.e_f147 + a.f148*b.e_f148 + a.f149*b.e_f149 + a.f150*b.e_f150 + a.f151*b.e_f151 + a.f152*b.e_f152 + a.f153*b.e_f153 + a.f154*b.e_f154 + a.f155*b.e_f155 + a.f156*b.e_f156 + a.f157*b.e_f157 + a.f158*b.e_f158 + a.f159*b.e_f159 + a.f160*b.e_f160 + a.f161*b.e_f161 + a.f162*b.e_f162 + a.f163*b.e_f163 + a.f164*b.e_f164 + a.f165*b.e_f165 + a.f166*b.e_f166 + a.f167*b.e_f167 + a.f168*b.e_f168 + a.f169*b.e_f169 + a.f170*b.e_f170 + a.f171*b.e_f171 + a.f172*b.e_f172 + a.f173*b.e_f173 + a.f174*b.e_f174 + a.f175*b.e_f175 + a.f176*b.e_f176 + a.f177*b.e_f177 + a.f178*b.e_f178 + a.f179*b.e_f179 + a.f180*b.e_f180 + a.f181*b.e_f181 + a.f182*b.e_f182 + a.f183*b.e_f183 + a.f184*b.e_f184 + a.f185*b.e_f185 + a.f186*b.e_f186 + a.f187*b.e_f187 + a.f188*b.e_f188 + a.f189*b.e_f189 + a.f190*b.e_f190 + a.f191*b.e_f191 + a.f192*b.e_f192 + a.f193*b.e_f193 + a.f194*b.e_f194 + a.f195*b.e_f195 + a.f196*b.e_f196 + a.f197*b.e_f197 + a.f198*b.e_f198 + a.f199*b.e_f199 + a.f200*b.e_f200 + a.f201*b.e_f201 + a.f202*b.e_f202 + a.f203*b.e_f203 + a.f204*b.e_f204 + a.f205*b.e_f205 + a.f206*b.e_f206 + a.f207*b.e_f207 + a.f208*b.e_f208 + a.f209*b.e_f209 + a.f210*b.e_f210 + a.f211*b.e_f211 + a.f212*b.e_f212 + a.f213*b.e_f213 + a.f214*b.e_f214 + a.f215*b.e_f215 + a.f216*b.e_f216 + a.f217*b.e_f217 + a.f218*b.e_f218 + a.f219*b.e_f219 + a.f220*b.e_f220 + a.f221*b.e_f221 + a.f222*b.e_f222 + a.f223*b.e_f223 + a.f224*b.e_f224 + a.f225*b.e_f225 + a.f226*b.e_f226 + a.f227*b.e_f227 + a.f228*b.e_f228 + a.f229*b.e_f229 + a.f230*b.e_f230 + a.f231*b.e_f231 + a.f232*b.e_f232 + a.f233*b.e_f233 + a.f234*b.e_f234 + a.f235*b.e_f235 + a.f236*b.e_f236 + a.f237*b.e_f237 + a.f238*b.e_f238 + a.f239*b.e_f239 + a.f240*b.e_f240 + a.f241*b.e_f241 + a.f242*b.e_f242 + a.f243*b.e_f243 + a.f244*b.e_f244 + a.f245*b.e_f245 + a.f246*b.e_f246 + a.f247*b.e_f247 + a.f248*b.e_f248 + a.f249*b.e_f249 + a.f250*b.e_f250 + a.f251*b.e_f251 + a.f252*b.e_f252 + a.f253*b.e_f253 + a.f254*b.e_f254 + a.f255*b.e_f255 + a.f256*b.e_f256 + a.f257*b.e_f257 + a.f258*b.e_f258 + a.f259*b.e_f259 + a.f260*b.e_f260 + a.f261*b.e_f261 + a.f262*b.e_f262 + a.f263*b.e_f263 + a.f264*b.e_f264 + a.f265*b.e_f265 + a.f266*b.e_f266 + a.f267*b.e_f267 + a.f268*b.e_f268 + a.f269*b.e_f269 + a.f270*b.e_f270 + a.f271*b.e_f271 + a.f272*b.e_f272 + a.f273*b.e_f273 + a.f274*b.e_f274 + a.f275*b.e_f275 + a.f276*b.e_f276 + a.f277*b.e_f277 + a.f278*b.e_f278 + a.f279*b.e_f279 + a.f280*b.e_f280 + a.f281*b.e_f281 + a.f282*b.e_f282 + a.f283*b.e_f283 + a.f284*b.e_f284 + a.f285*b.e_f285 + a.f286*b.e_f286 + a.f287*b.e_f287 + a.f288*b.e_f288 + a.f289*b.e_f289 + a.f290*b.e_f290 + a.f291*b.e_f291 + a.f292*b.e_f292 + a.f293*b.e_f293 + a.f294*b.e_f294 + a.f295*b.e_f295 + a.f296*b.e_f296 + a.f297*b.e_f297 + a.f298*b.e_f298 + a.f299*b.e_f299 + a.f300*b.e_f300), (SQRT(a.f1*a.f1 + a.f2*a.f2 + a.f3*a.f3 + a.f4*a.f4 + a.f5*a.f5 + a.f6*a.f6 + a.f7*a.f7 + a.f8*a.f8 + a.f9*a.f9 + a.f10*a.f10 + a.f11*a.f11 + a.f12*a.f12 + a.f13*a.f13 + a.f14*a.f14 + a.f15*a.f15 + a.f16*a.f16 + a.f17*a.f17 + a.f18*a.f18 + a.f19*a.f19 + a.f20*a.f20 + a.f21*a.f21 + a.f22*a.f22 + a.f23*a.f23 + a.f24*a.f24 + a.f25*a.f25 + a.f26*a.f26 + a.f27*a.f27 + a.f28*a.f28 + a.f29*a.f29 + a.f30*a.f30 + a.f31*a.f31 + a.f32*a.f32 + a.f33*a.f33 + a.f34*a.f34 + a.f35*a.f35 + a.f36*a.f36 + a.f37*a.f37 + a.f38*a.f38 + a.f39*a.f39 + a.f40*a.f40 + a.f41*a.f41 + a.f42*a.f42 + a.f43*a.f43 + a.f44*a.f44 + a.f45*a.f45 + a.f46*a.f46 + a.f47*a.f47 + a.f48*a.f48 + a.f49*a.f49 + a.f50*a.f50 + a.f51*a.f51 + a.f52*a.f52 + a.f53*a.f53 + a.f54*a.f54 + a.f55*a.f55 + a.f56*a.f56 + a.f57*a.f57 + a.f58*a.f58 + a.f59*a.f59 + a.f60*a.f60 + a.f61*a.f61 + a.f62*a.f62 + a.f63*a.f63 + a.f64*a.f64 + a.f65*a.f65 + a.f66*a.f66 + a.f67*a.f67 + a.f68*a.f68 + a.f69*a.f69 + a.f70*a.f70 + a.f71*a.f71 + a.f72*a.f72 + a.f73*a.f73 + a.f74*a.f74 + a.f75*a.f75 + a.f76*a.f76 + a.f77*a.f77 + a.f78*a.f78 + a.f79*a.f79 + a.f80*a.f80 + a.f81*a.f81 + a.f82*a.f82 + a.f83*a.f83 + a.f84*a.f84 + a.f85*a.f85 + a.f86*a.f86 + a.f87*a.f87 + a.f88*a.f88 + a.f89*a.f89 + a.f90*a.f90 + a.f91*a.f91 + a.f92*a.f92 + a.f93*a.f93 + a.f94*a.f94 + a.f95*a.f95 + a.f96*a.f96 + a.f97*a.f97 + a.f98*a.f98 + a.f99*a.f99 + a.f100*a.f100 + a.f101*a.f101 + a.f102*a.f102 + a.f103*a.f103 + a.f104*a.f104 + a.f105*a.f105 + a.f106*a.f106 + a.f107*a.f107 + a.f108*a.f108 + a.f109*a.f109 + a.f110*a.f110 + a.f111*a.f111 + a.f112*a.f112 + a.f113*a.f113 + a.f114*a.f114 + a.f115*a.f115 + a.f116*a.f116 + a.f117*a.f117 + a.f118*a.f118 + a.f119*a.f119 + a.f120*a.f120 + a.f121*a.f121 + a.f122*a.f122 + a.f123*a.f123 + a.f124*a.f124 + a.f125*a.f125 + a.f126*a.f126 + a.f127*a.f127 + a.f128*a.f128 + a.f129*a.f129 + a.f130*a.f130 + a.f131*a.f131 + a.f132*a.f132 + a.f133*a.f133 + a.f134*a.f134 + a.f135*a.f135 + a.f136*a.f136 + a.f137*a.f137 + a.f138*a.f138 + a.f139*a.f139 + a.f140*a.f140 + a.f141*a.f141 + a.f142*a.f142 + a.f143*a.f143 + a.f144*a.f144 + a.f145*a.f145 + a.f146*a.f146 + a.f147*a.f147 + a.f148*a.f148 + a.f149*a.f149 + a.f150*a.f150 + a.f151*a.f151 + a.f152*a.f152 + a.f153*a.f153 + a.f154*a.f154 + a.f155*a.f155 + a.f156*a.f156 + a.f157*a.f157 + a.f158*a.f158 + a.f159*a.f159 + a.f160*a.f160 + a.f161*a.f161 + a.f162*a.f162 + a.f163*a.f163 + a.f164*a.f164 + a.f165*a.f165 + a.f166*a.f166 + a.f167*a.f167 + a.f168*a.f168 + a.f169*a.f169 + a.f170*a.f170 + a.f171*a.f171 + a.f172*a.f172 + a.f173*a.f173 + a.f174*a.f174 + a.f175*a.f175 + a.f176*a.f176 + a.f177*a.f177 + a.f178*a.f178 + a.f179*a.f179 + a.f180*a.f180 + a.f181*a.f181 + a.f182*a.f182 + a.f183*a.f183 + a.f184*a.f184 + a.f185*a.f185 + a.f186*a.f186 + a.f187*a.f187 + a.f188*a.f188 + a.f189*a.f189 + a.f190*a.f190 + a.f191*a.f191 + a.f192*a.f192 + a.f193*a.f193 + a.f194*a.f194 + a.f195*a.f195 + a.f196*a.f196 + a.f197*a.f197 + a.f198*a.f198 + a.f199*a.f199 + a.f200*a.f200 + a.f201*a.f201 + a.f202*a.f202 + a.f203*a.f203 + a.f204*a.f204 + a.f205*a.f205 + a.f206*a.f206 + a.f207*a.f207 + a.f208*a.f208 + a.f209*a.f209 + a.f210*a.f210 + a.f211*a.f211 + a.f212*a.f212 + a.f213*a.f213 + a.f214*a.f214 + a.f215*a.f215 + a.f216*a.f216 + a.f217*a.f217 + a.f218*a.f218 + a.f219*a.f219 + a.f220*a.f220 + a.f221*a.f221 + a.f222*a.f222 + a.f223*a.f223 + a.f224*a.f224 + a.f225*a.f225 + a.f226*a.f226 + a.f227*a.f227 + a.f228*a.f228 + a.f229*a.f229 + a.f230*a.f230 + a.f231*a.f231 + a.f232*a.f232 + a.f233*a.f233 + a.f234*a.f234 + a.f235*a.f235 + a.f236*a.f236 + a.f237*a.f237 + a.f238*a.f238 + a.f239*a.f239 + a.f240*a.f240 + a.f241*a.f241 + a.f242*a.f242 + a.f243*a.f243 + a.f244*a.f244 + a.f245*a.f245 + a.f246*a.f246 + a.f247*a.f247 + a.f248*a.f248 + a.f249*a.f249 + a.f250*a.f250 + a.f251*a.f251 + a.f252*a.f252 + a.f253*a.f253 + a.f254*a.f254 + a.f255*a.f255 + a.f256*a.f256 + a.f257*a.f257 + a.f258*a.f258 + a.f259*a.f259 + a.f260*a.f260 + a.f261*a.f261 + a.f262*a.f262 + a.f263*a.f263 + a.f264*a.f264 + a.f265*a.f265 + a.f266*a.f266 + a.f267*a.f267 + a.f268*a.f268 + a.f269*a.f269 + a.f270*a.f270 + a.f271*a.f271 + a.f272*a.f272 + a.f273*a.f273 + a.f274*a.f274 + a.f275*a.f275 + a.f276*a.f276 + a.f277*a.f277 + a.f278*a.f278 + a.f279*a.f279 + a.f280*a.f280 + a.f281*a.f281 + a.f282*a.f282 + a.f283*a.f283 + a.f284*a.f284 + a.f285*a.f285 + a.f286*a.f286 + a.f287*a.f287 + a.f288*a.f288 + a.f289*a.f289 + a.f290*a.f290 + a.f291*a.f291 + a.f292*a.f292 + a.f293*a.f293 + a.f294*a.f294 + a.f295*a.f295 + a.f296*a.f296 + a.f297*a.f297 + a.f298*a.f298 + a.f299*a.f299 + a.f300*a.f300)*SQRT(b.e_f1*b.e_f1 + b.e_f2*b.e_f2 + b.e_f3*b.e_f3 + b.e_f4*b.e_f4 + b.e_f5*b.e_f5 + b.e_f6*b.e_f6 + b.e_f7*b.e_f7 + b.e_f8*b.e_f8 + b.e_f9*b.e_f9 + b.e_f10*b.e_f10 + b.e_f11*b.e_f11 + b.e_f12*b.e_f12 + b.e_f13*b.e_f13 + b.e_f14*b.e_f14 + b.e_f15*b.e_f15 + b.e_f16*b.e_f16 + b.e_f17*b.e_f17 + b.e_f18*b.e_f18 + b.e_f19*b.e_f19 + b.e_f20*b.e_f20 + b.e_f21*b.e_f21 + b.e_f22*b.e_f22 + b.e_f23*b.e_f23 + b.e_f24*b.e_f24 + b.e_f25*b.e_f25 + b.e_f26*b.e_f26 + b.e_f27*b.e_f27 + b.e_f28*b.e_f28 + b.e_f29*b.e_f29 + b.e_f30*b.e_f30 + b.e_f31*b.e_f31 + b.e_f32*b.e_f32 + b.e_f33*b.e_f33 + b.e_f34*b.e_f34 + b.e_f35*b.e_f35 + b.e_f36*b.e_f36 + b.e_f37*b.e_f37 + b.e_f38*b.e_f38 + b.e_f39*b.e_f39 + b.e_f40*b.e_f40 + b.e_f41*b.e_f41 + b.e_f42*b.e_f42 + b.e_f43*b.e_f43 + b.e_f44*b.e_f44 + b.e_f45*b.e_f45 + b.e_f46*b.e_f46 + b.e_f47*b.e_f47 + b.e_f48*b.e_f48 + b.e_f49*b.e_f49 + b.e_f50*b.e_f50 + b.e_f51*b.e_f51 + b.e_f52*b.e_f52 + b.e_f53*b.e_f53 + b.e_f54*b.e_f54 + b.e_f55*b.e_f55 + b.e_f56*b.e_f56 + b.e_f57*b.e_f57 + b.e_f58*b.e_f58 + b.e_f59*b.e_f59 + b.e_f60*b.e_f60 + b.e_f61*b.e_f61 + b.e_f62*b.e_f62 + b.e_f63*b.e_f63 + b.e_f64*b.e_f64 + b.e_f65*b.e_f65 + b.e_f66*b.e_f66 + b.e_f67*b.e_f67 + b.e_f68*b.e_f68 + b.e_f69*b.e_f69 + b.e_f70*b.e_f70 + b.e_f71*b.e_f71 + b.e_f72*b.e_f72 + b.e_f73*b.e_f73 + b.e_f74*b.e_f74 + b.e_f75*b.e_f75 + b.e_f76*b.e_f76 + b.e_f77*b.e_f77 + b.e_f78*b.e_f78 + b.e_f79*b.e_f79 + b.e_f80*b.e_f80 + b.e_f81*b.e_f81 + b.e_f82*b.e_f82 + b.e_f83*b.e_f83 + b.e_f84*b.e_f84 + b.e_f85*b.e_f85 + b.e_f86*b.e_f86 + b.e_f87*b.e_f87 + b.e_f88*b.e_f88 + b.e_f89*b.e_f89 + b.e_f90*b.e_f90 + b.e_f91*b.e_f91 + b.e_f92*b.e_f92 + b.e_f93*b.e_f93 + b.e_f94*b.e_f94 + b.e_f95*b.e_f95 + b.e_f96*b.e_f96 + b.e_f97*b.e_f97 + b.e_f98*b.e_f98 + b.e_f99*b.e_f99 + b.e_f100*b.e_f100 + b.e_f101*b.e_f101 + b.e_f102*b.e_f102 + b.e_f103*b.e_f103 + b.e_f104*b.e_f104 + b.e_f105*b.e_f105 + b.e_f106*b.e_f106 + b.e_f107*b.e_f107 + b.e_f108*b.e_f108 + b.e_f109*b.e_f109 + b.e_f110*b.e_f110 + b.e_f111*b.e_f111 + b.e_f112*b.e_f112 + b.e_f113*b.e_f113 + b.e_f114*b.e_f114 + b.e_f115*b.e_f115 + b.e_f116*b.e_f116 + b.e_f117*b.e_f117 + b.e_f118*b.e_f118 + b.e_f119*b.e_f119 + b.e_f120*b.e_f120 + b.e_f121*b.e_f121 + b.e_f122*b.e_f122 + b.e_f123*b.e_f123 + b.e_f124*b.e_f124 + b.e_f125*b.e_f125 + b.e_f126*b.e_f126 + b.e_f127*b.e_f127 + b.e_f128*b.e_f128 + b.e_f129*b.e_f129 + b.e_f130*b.e_f130 + b.e_f131*b.e_f131 + b.e_f132*b.e_f132 + b.e_f133*b.e_f133 + b.e_f134*b.e_f134 + b.e_f135*b.e_f135 + b.e_f136*b.e_f136 + b.e_f137*b.e_f137 + b.e_f138*b.e_f138 + b.e_f139*b.e_f139 + b.e_f140*b.e_f140 + b.e_f141*b.e_f141 + b.e_f142*b.e_f142 + b.e_f143*b.e_f143 + b.e_f144*b.e_f144 + b.e_f145*b.e_f145 + b.e_f146*b.e_f146 + b.e_f147*b.e_f147 + b.e_f148*b.e_f148 + b.e_f149*b.e_f149 + b.e_f150*b.e_f150 + b.e_f151*b.e_f151 + b.e_f152*b.e_f152 + b.e_f153*b.e_f153 + b.e_f154*b.e_f154 + b.e_f155*b.e_f155 + b.e_f156*b.e_f156 + b.e_f157*b.e_f157 + b.e_f158*b.e_f158 + b.e_f159*b.e_f159 + b.e_f160*b.e_f160 + b.e_f161*b.e_f161 + b.e_f162*b.e_f162 + b.e_f163*b.e_f163 + b.e_f164*b.e_f164 + b.e_f165*b.e_f165 + b.e_f166*b.e_f166 + b.e_f167*b.e_f167 + b.e_f168*b.e_f168 + b.e_f169*b.e_f169 + b.e_f170*b.e_f170 + b.e_f171*b.e_f171 + b.e_f172*b.e_f172 + b.e_f173*b.e_f173 + b.e_f174*b.e_f174 + b.e_f175*b.e_f175 + b.e_f176*b.e_f176 + b.e_f177*b.e_f177 + b.e_f178*b.e_f178 + b.e_f179*b.e_f179 + b.e_f180*b.e_f180 + b.e_f181*b.e_f181 + b.e_f182*b.e_f182 + b.e_f183*b.e_f183 + b.e_f184*b.e_f184 + b.e_f185*b.e_f185 + b.e_f186*b.e_f186 + b.e_f187*b.e_f187 + b.e_f188*b.e_f188 + b.e_f189*b.e_f189 + b.e_f190*b.e_f190 + b.e_f191*b.e_f191 + b.e_f192*b.e_f192 + b.e_f193*b.e_f193 + b.e_f194*b.e_f194 + b.e_f195*b.e_f195 + b.e_f196*b.e_f196 + b.e_f197*b.e_f197 + b.e_f198*b.e_f198 + b.e_f199*b.e_f199 + b.e_f200*b.e_f200 + b.e_f201*b.e_f201 + b.e_f202*b.e_f202 + b.e_f203*b.e_f203 + b.e_f204*b.e_f204 + b.e_f205*b.e_f205 + b.e_f206*b.e_f206 + b.e_f207*b.e_f207 + b.e_f208*b.e_f208 + b.e_f209*b.e_f209 + b.e_f210*b.e_f210 + b.e_f211*b.e_f211 + b.e_f212*b.e_f212 + b.e_f213*b.e_f213 + b.e_f214*b.e_f214 + b.e_f215*b.e_f215 + b.e_f216*b.e_f216 + b.e_f217*b.e_f217 + b.e_f218*b.e_f218 + b.e_f219*b.e_f219 + b.e_f220*b.e_f220 + b.e_f221*b.e_f221 + b.e_f222*b.e_f222 + b.e_f223*b.e_f223 + b.e_f224*b.e_f224 + b.e_f225*b.e_f225 + b.e_f226*b.e_f226 + b.e_f227*b.e_f227 + b.e_f228*b.e_f228 + b.e_f229*b.e_f229 + b.e_f230*b.e_f230 + b.e_f231*b.e_f231 + b.e_f232*b.e_f232 + b.e_f233*b.e_f233 + b.e_f234*b.e_f234 + b.e_f235*b.e_f235 + b.e_f236*b.e_f236 + b.e_f237*b.e_f237 + b.e_f238*b.e_f238 + b.e_f239*b.e_f239 + b.e_f240*b.e_f240 + b.e_f241*b.e_f241 + b.e_f242*b.e_f242 + b.e_f243*b.e_f243 + b.e_f244*b.e_f244 + b.e_f245*b.e_f245 + b.e_f246*b.e_f246 + b.e_f247*b.e_f247 + b.e_f248*b.e_f248 + b.e_f249*b.e_f249 + b.e_f250*b.e_f250 + b.e_f251*b.e_f251 + b.e_f252*b.e_f252 + b.e_f253*b.e_f253 + b.e_f254*b.e_f254 + b.e_f255*b.e_f255 + b.e_f256*b.e_f256 + b.e_f257*b.e_f257 + b.e_f258*b.e_f258 + b.e_f259*b.e_f259 + b.e_f260*b.e_f260 + b.e_f261*b.e_f261 + b.e_f262*b.e_f262 + b.e_f263*b.e_f263 + b.e_f264*b.e_f264 + b.e_f265*b.e_f265 + b.e_f266*b.e_f266 + b.e_f267*b.e_f267 + b.e_f268*b.e_f268 + b.e_f269*b.e_f269 + b.e_f270*b.e_f270 + b.e_f271*b.e_f271 + b.e_f272*b.e_f272 + b.e_f273*b.e_f273 + b.e_f274*b.e_f274 + b.e_f275*b.e_f275 + b.e_f276*b.e_f276 + b.e_f277*b.e_f277 + b.e_f278*b.e_f278 + b.e_f279*b.e_f279 + b.e_f280*b.e_f280 + b.e_f281*b.e_f281 + b.e_f282*b.e_f282 + b.e_f283*b.e_f283 + b.e_f284*b.e_f284 + b.e_f285*b.e_f285 + b.e_f286*b.e_f286 + b.e_f287*b.e_f287 + b.e_f288*b.e_f288 + b.e_f289*b.e_f289 + b.e_f290*b.e_f290 + b.e_f291*b.e_f291 + b.e_f292*b.e_f292 + b.e_f293*b.e_f293 + b.e_f294*b.e_f294 + b.e_f295*b.e_f295 + b.e_f296*b.e_f296 + b.e_f297*b.e_f297 + b.e_f298*b.e_f298 + b.e_f299*b.e_f299 + b.e_f300*b.e_f300))) similarity
FROM `gcp-pdp-osm-dev.osm_clustering.w2v_glove_6B_300d` a
CROSS JOIN `gcp-pdp-osm-dev.osm_clustering.grid_1km_300d_clusters_10_centroids_trans` b
WHERE b.centroid_id = 5
ORDER BY similarity DESC
LIMIT 20
```