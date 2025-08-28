-- This incremental table contains the base geometry for the clusters.
-- This geometry depends on the pixel threshold, which is a variable parameter that is passed here from the orchestrator.
-- The table is incremental, so every time a new pixel threshold is passed, the table is updated rather than re-created.
-- If we pass an already existing pixel threshold, the rows corresponding to that pixel threshold are deleted and re-created.
-- If we pass a new pixel threshold, the rows corresponding to that pixel threshold are created.

{{ config(
    materialized = 'incremental',
    strategy = 'delete+insert',
    pre_hook = "delete from {{ this }} where pixel_threshold = {{ var('pixel_threshold') | int }}",
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
)}}

{% set years = var('constants')['USA_YEARS'] %}
{% set dbscan_eps = var('constants')['USA_DB_SCAN_EPS'] %}
{% set dbscan_min_points = var('constants')['USA_DB_SCAN_MIN_POINTS'] %}

-- Pixel threshold is a variable parameter that is passed here from the orchestrator
{% set pixel_threshold = var('pixel_threshold') | int %}

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
            geom
    FROM cluster_{{ year }}

    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %}
)
SELECT * FROM cluster_union