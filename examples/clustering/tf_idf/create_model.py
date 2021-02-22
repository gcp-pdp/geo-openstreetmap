CREATE_MODEL_STATEMENT = """
CREATE OR REPLACE MODEL
  osm_clustering_grid_1km.kmeans_tfidf_clusters_10 
TRANSFORM(
  {}
)
OPTIONS(model_type='kmeans', num_clusters=10, max_iterations=50, EARLY_STOP=TRUE, MIN_REL_PROGRESS=0.001) AS
SELECT
  tfidf_vec
FROM
  osm_clustering_grid_1km.vectors_tfidf
"""
DIMENSIONALITY = 339
create_model = CREATE_MODEL_STATEMENT.format(
    ', '.join(['tfidf_vec[OFFSET({})] as f{}'.format(i, i + 1) for i in range(DIMENSIONALITY)]))
print(create_model)
