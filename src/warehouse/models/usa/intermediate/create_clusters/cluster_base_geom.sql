{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
)}}

{% set years = var('USA_YEARS') %}
{% set dbscan_eps = var('USA_DB_SCAN_EPS') %}
{% set dbscan_min_points = var('USA_DB_SCAN_MIN_POINTS') %}
{% set pixel_threshold = var('USA_PIXEL_THRESHOLD') %}

WITH 
{% for year in years %}
    cluster_{{ year }} AS (
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
        )
        SELECT cid AS cluster_id, ST_Union(geom) AS geom
        FROM dbscan
        GROUP BY cid
    ),
{% endfor %}
cluster_union AS (
{% for year in years %}
    SELECT  CONCAT({{year}}, '-', LPAD(cluster_id::text, 4, '0')) AS cluster_id, 
            {{ year }} AS year, 
            {{ pixel_threshold }} AS pixel_threshold,
            geom,
    FROM cluster_{{ year }}

    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %}
)
SELECT * FROM cluster_union