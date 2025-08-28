{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
)}}

WITH cluster_growth_without_id AS (
    SELECT  cw.component_id,
            cw.y1,
            cw.y2, 
            cw.pixel_threshold,
            ST_Union(geom) AS geom
    FROM {{ source('matching', 'usa_crosswalk_component_id_to_cluster_id') }} cw
    INNER JOIN {{ ref('usa_cluster_base_geom') }} cg
    ON cw.cluster_id = cg.cluster_id
    --- The cluster_id in the usa_cluster_base_geom is a concatenation of the year, pixel_threshold, and cluster_order_id.
    --- So it is unique for any given cluster, implying that we can use only the cluster_id to join. 
    GROUP BY cw.component_id, cw.y1, cw.y2, cw.pixel_threshold
),
cluster_growth_ranked AS (
        SELECT  component_id,
                y1,
                y2,
                pixel_threshold,
                geom,
                ROUND(ST_X(ST_Transform(ST_Centroid(geom), 4326))::numeric, 6) AS cx,
                ROUND(ST_Y(ST_Transform(ST_Centroid(geom), 4326))::numeric, 6) AS cy,
                ST_Area(geom) AS area
        FROM cluster_growth_without_id
),
cluster_growth_ordered AS (
    SELECT  component_id,
            y1,
            y2,
            pixel_threshold,
            geom,
            ROW_NUMBER() OVER (PARTITION BY y1, y2, pixel_threshold ORDER BY cx, cy, area DESC) AS cluster_order_id
    FROM cluster_growth_ranked
),
cluster_growth_final AS (
    SELECT  CONCAT(y1, '-', y2, '-', pixel_threshold, '-', LPAD(cluster_order_id::text, 4, '0')) AS cluster_id,
            y1,
            y2,
            pixel_threshold, 
            geom
    FROM cluster_growth_ordered
)
SELECT * FROM cluster_growth_final
ORDER BY cluster_id, pixel_threshold, y1, y2
