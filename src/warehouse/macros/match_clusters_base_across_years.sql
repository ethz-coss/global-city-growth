{% macro match_clusters_base_across_years(cluster_geom_table) %}
WITH year_list AS (
    SELECT DISTINCT year
    FROM {{ cluster_geom_table }}
),
decade_gap_pairs AS (
    SELECT yl1.year AS y1, yl2.year AS y2
    FROM year_list yl1
    CROSS JOIN year_list yl2
    WHERE yl1.year + 10 = yl2.year
),
matching_decade_gap_pairs AS (
    SELECT  p.y1,
            p.y2,
            l.urban_threshold AS urban_threshold,
            l.cluster_id AS left_cluster_id,
            r.cluster_id AS right_cluster_id
    FROM decade_gap_pairs p
    INNER JOIN {{ cluster_geom_table }} l
    ON l.year = p.y1
    LEFT JOIN {{ cluster_geom_table }} r
    ON r.year = p.y2
    AND r.urban_threshold = l.urban_threshold
    AND r.geom && l.geom
    AND ST_Intersects(r.geom, l.geom)
),
matching_same_year AS (
    SELECT year AS y1, 
           year AS y2, 
           urban_threshold, 
           cluster_id AS left_cluster_id, 
           cluster_id AS right_cluster_id
    FROM {{ cluster_geom_table }}
)
SELECT * FROM matching_decade_gap_pairs
UNION ALL
SELECT * FROM matching_same_year
{% endmacro %}