{% macro match_clusters_base_across_years(cluster_geom_table) %}
WITH year_list AS (
    SELECT DISTINCT year
    FROM {{ cluster_geom_table }}
),
year_pairs AS (
    WITH base_pairs AS (
        SELECT year AS y1, LEAD(year) OVER (ORDER BY year) AS y2
        FROM year_list
        UNION ALL
        SELECT year AS y1, year AS y2
        FROM year_list
    ),
    filtered_pairs AS (
        SELECT y1, y2
        FROM base_pairs
        WHERE y2 IS NOT NULL
        AND (y1 = y2 OR y1 + 10 = y2)
    )
    SELECT * FROM filtered_pairs ORDER BY y1, y2
)
SELECT  p.y1,
        p.y2,
        l.urban_threshold AS urban_threshold,
        l.cluster_id AS left_cluster_id,
        r.cluster_id AS right_cluster_id
FROM year_pairs p
INNER JOIN {{ cluster_geom_table }} l
ON l.year = p.y1
LEFT JOIN {{ cluster_geom_table }} r
ON r.year = p.y2
AND r.urban_threshold = l.urban_threshold
AND l.geom && r.geom --- Cheap bounding box intersection check speeds up the query
AND ST_Intersects(r.geom, l.geom)
{% endmacro %}