QUERY = """
SELECT 
    grid.geohash,
    grid.geog,
    grid.geohash_l2_decimal,
    lbcs_a.name activity_name,
    lbcs_f.name function_name,
    colorscheme.color,
    similarities.max_similarity_a similarity_a,
    similarities.max_similarity_f similarity_f
FROM `gcp-pdp-osm-dev.geohash.level_7_partitioned` AS grid
JOIN `gcp-pdp-osm-dev.geohash.level_7_lbcs_similarities_partitioned` AS similarities USING(geohash)
JOIN `gcp-pdp-osm-dev.geohash.level_7_vectors_tfidf_partitioned` AS tfidf USING(geohash)
JOIN `gcp-pdp-osm-dev.lbcs.lbcs_tfidf_layers` AS lbcs_a
    ON udfs.cosine_similarity(lbcs_a.tfidf_vec, tfidf.tfidf_vec) = similarities.max_similarity_a
    AND lbcs_a.dimension = "Activity"
JOIN `gcp-pdp-osm-dev.lbcs.lbcs_tfidf_layers` AS lbcs_f
    ON udfs.cosine_similarity(lbcs_f.tfidf_vec, tfidf.tfidf_vec) = similarities.max_similarity_f
    AND lbcs_f.dimension = "Function"
JOIN `gcp-pdp-osm-dev.lbcs.lbcs_activity_and_function` AS colorscheme
ON colorscheme.activity = lbcs_a.name AND colorscheme.function = lbcs_f.name
WHERE grid.geohash_l2_decimal = {0} AND similarities.geohash_l2_decimal = {0} AND tfidf.geohash_l2_decimal = {0}
"""

CMD = 'bq query --append_table --destination_table gcp-pdp-osm-dev:geohash.level_7_results_partitioned' \
      ' --{}synchronous_mode --nouse_legacy_sql \'{}\''
EXCLUDE = (
    0, 1, 2, 3, 4, 6, 8, 9, 10, 12, 14, 16, 32, 33, 34, 35, 36, 38, 40, 41, 42, 43, 44, 45, 46, 47, 55, 64, 68, 71, 77,
    79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 101, 108, 109, 127, 128, 129, 130, 131, 132, 133,
    134, 135, 136, 137, 138, 139, 140, 141, 142, 146, 147, 150, 151, 152, 153, 154, 155, 156, 157, 160, 161, 162, 163,
    164, 166, 168, 169, 170, 171, 172, 173, 174, 175, 177, 179, 180, 184, 185, 186, 187, 193, 194, 195, 196, 197, 198,
    199, 200, 201, 203, 204, 205, 206, 207, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224,
    229, 233, 234, 240, 241, 242, 244, 245, 247, 251, 252, 256, 258, 259, 260, 263, 264, 269, 272, 273, 274, 295, 297,
    298, 299, 301, 302, 303, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 321, 323, 324, 325,
    326, 327, 329, 332, 333, 334, 335, 336, 338, 341, 343, 344, 346, 349, 351, 352, 353, 354, 355, 356, 357, 358, 359,
    360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 373, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384,
    385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409,
    410, 412, 413, 415, 418, 422, 423, 425, 426, 427, 428, 429, 430, 431, 435, 436, 437, 438, 439, 440, 441, 442, 443,
    444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 466,
    467, 468, 469, 470, 471, 472, 473, 474, 475, 476, 477, 478, 479, 484, 485, 486, 487, 488, 490, 491, 492, 493, 494,
    495, 496, 497, 498, 499, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516, 517,
    518, 519, 520, 529, 551, 606, 628, 642, 646, 647, 656, 658, 674, 676, 680, 681, 687, 693, 702, 711, 715, 717, 742,
    756, 776, 778, 815, 844, 863, 871, 877, 939, 940, 944, 949, 966, 967, 968, 994, 997, 1016
)
if __name__ == '__main__':
    for i in range(1024):
        if i in EXCLUDE:
            continue
        query = QUERY.format(i).replace('\n', ' ')
        sync = 'no'
        if i % 5 == 0:
            sync = ''
        cmd = CMD.format(sync, query)
        print(cmd)
