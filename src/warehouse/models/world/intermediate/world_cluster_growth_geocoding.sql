WITH cluster_country_matching AS (
    SELECT  clst.cluster_id, 
            clst.y1,
            clst.y2,
            clst.urban_threshold,
            ctry.gwcode
    FROM {{ ref('world_cluster_growth_geom') }} clst
    JOIN {{ ref('world_country_borders_2019') }} ctry
    ON ST_Intersects(clst.geom, ctry.geom)
),
countries_matched_per_cluster AS (
    SELECT cluster_id, y1, y2, urban_threshold, COUNT(*) AS n_matched_countries
    FROM cluster_country_matching
    GROUP BY cluster_id, y1, y2, urban_threshold
),
cluster_country_matching_with_country_match_count AS (
    SELECT  m.cluster_id, 
            m.y1,
            m.y2,
            m.urban_threshold,
            m.gwcode,
            cmpc.n_matched_countries
    FROM cluster_country_matching m
    INNER JOIN countries_matched_per_cluster cmpc
    USING (cluster_id, y1, y2, urban_threshold)
)
SELECT * FROM cluster_country_matching_with_country_match_count