WITH l2 AS (SELECT * FROM `gcp-pdp-osm-dev.geohash.level_2`)
,l7 AS (SELECT * FROM `gcp-pdp-osm-dev.geohash.level_7`)
,alphabet AS (
SELECT symbol, offset FROM UNNEST([
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q',
    'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']) as symbol
WITH OFFSET AS offset
ORDER BY offset)
,l2_symbols AS (SELECT l2.geohash, SPLIT(REVERSE(l2.geohash), '') AS symbols FROM l2)
,symbol_values AS (SELECT l2_symbols.geohash, symbol, offset as idx, alphabet.offset as decimal, alphabet.offset*POW(32, offset) as decimal_value
FROM l2_symbols
CROSS JOIN UNNEST(l2_symbols.symbols) as symbol WITH OFFSET AS offset
JOIN alphabet ON alphabet.symbol = symbol)
,l2_decimal_values AS (SELECT geohash, CAST(SUM(decimal_value) AS INT64) AS value
FROM symbol_values
GROUP BY geohash)
SELECT l7.*, l2_decimal_values.value as geohash_l2_decimal
FROM l7
JOIN l2_decimal_values ON l2_decimal_values.geohash = SUBSTR(l7.geohash, 1, 2)