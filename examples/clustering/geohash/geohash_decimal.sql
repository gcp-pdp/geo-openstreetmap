WITH geohashes AS (SELECT geohash
  FROM UNNEST(["u0","w1","00","yz", "zzz"]) AS geohash)
,alphabet AS (
SELECT symbol, offset FROM UNNEST([
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q',
    'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']) as symbol
WITH OFFSET AS offset
ORDER BY offset)
,geohash_letters AS (SELECT geohashes.geohash, SPLIT(REVERSE(geohashes.geohash), '') AS symbols FROM geohashes)
,decimal_values AS(SELECT geohash_letters.geohash, symbol, offset as idx, alphabet.offset as decimal, POW(alphabet.offset, offset + 1) as decimal_value
FROM geohash_letters
CROSS JOIN UNNEST(geohash_letters.symbols) as symbol WITH OFFSET AS offset
JOIN alphabet ON alphabet.symbol = symbol)
SELECT geohash, CAST(SUM(decimal_value) AS INT64) AS value
FROM decimal_values
GROUP BY geohash