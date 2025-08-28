{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
)}}

{% set years = var('constants')['USA_YEARS'] %}
{% set dbscan_eps = var('constants')['USA_DB_SCAN_EPS'] %}
{% set dbscan_min_points = var('constants')['USA_DB_SCAN_MIN_POINTS'] %}
{% set pixel_thresholds = var('constants')['USA_PIXEL_THRESHOLDS'] %}

WITH 
{% for year in years %}
{% for pixel_threshold in pixel_thresholds %}
    cluster_{{ year }}_{{ pixel_threshold }} AS (
        WITH pixels AS (
            SELECT (ST_PixelAsPolygons(rast, 1, TRUE)).*
            FROM {{ source('convolved_raster', 'usa_raster_census_place_convolved') }}
            WHERE year = {{ year }}
        ),
        filtered_pixel AS (
            SELECT val, geom
            FROM pixels
            WHERE val > {{ pixel_threshold }}
        ),
        dbscan AS (
            SELECT ST_ClusterDBSCAN(geom, eps := {{ dbscan_eps }}, minpoints := {{ dbscan_min_points}}) OVER () AS cid, geom, val
            FROM filtered_pixel
        ),
        cluster_raw AS (
            SELECT cid, ST_Union(geom) AS geom
            FROM dbscan
            GROUP BY cid
        ),
        -- Here we rank the clusters by their centroid and area to give them a unique and deterministic id.
        cluster_ranked AS (
            SELECT  cid,
                    ROUND(ST_X(ST_Transform(ST_Centroid(geom), 4326))::numeric, 6) AS cx,
                    ROUND(ST_Y(ST_Transform(ST_Centroid(geom), 4326))::numeric, 6) AS cy,
                    ST_Area(geom) AS area,
                    geom
            FROM cluster_raw
        ),
        cluster_ordered AS (
            SELECT  ROW_NUMBER() OVER (ORDER BY cx, cy, area DESC) AS cluster_order_id,
                    geom
            FROM cluster_ranked
        ),
        cluster_final AS (
            SELECT  CONCAT({{ year }}, '-', {{ pixel_threshold }}, '-', LPAD(cluster_order_id::text, 4, '0')) AS cluster_id,
                    {{ year }} AS year,
                    {{ pixel_threshold }} AS pixel_threshold,
                    geom
            FROM cluster_ordered
        )
        SELECT * FROM cluster_final
    ),
{% endfor %}
{% endfor %}
cluster_union AS (
    {% set queries = [] %}
    {% for year in years %}
    {% for pixel_threshold in pixel_thresholds %}
        {% set query = "SELECT cluster_id, year, pixel_threshold, geom FROM cluster_" ~ year ~ "_" ~ pixel_threshold %}
        {% do queries.append(query) %}
    {% endfor %}
    {% endfor %}
    {{ queries | join(' UNION ALL\n') }}
)
SELECT * FROM cluster_union