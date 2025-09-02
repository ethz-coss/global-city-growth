-- This macro is used to create urban clusters from a raster table.
{% macro create_clusters_base_from_raster(raster_table, year, urban_threshold, dbscan_eps, dbscan_minpoints) %}
WITH binary_raster AS (
    SELECT ST_Reclass({{ raster_table }}.rast, 1, '[0-{{ urban_threshold }}]\:0, ({{ urban_threshold }}-100000000]\:1', '1BB', nodataval := 0) AS rast
    FROM {{ raster_table }}
    WHERE year = {{ year }}
),
urban_pixels AS (
    SELECT (ST_PixelAsPolygons(rast, 1, TRUE)).*
    FROM binary_raster
),
dbscan AS (
    SELECT ST_ClusterDBSCAN(geom, eps := {{ dbscan_eps }}, minpoints := {{ dbscan_minpoints }}) OVER () AS cid, geom
    FROM urban_pixels
),
cluster_raw AS (
    SELECT cid, ST_Union(geom) AS geom
    FROM dbscan
    GROUP BY cid
),
cluster_ranked AS (
    SELECT
        cid,
        ROUND(ST_X(ST_Transform(ST_Centroid(geom), 4326))::numeric, 6) AS cx,
        ROUND(ST_Y(ST_Transform(ST_Centroid(geom), 4326))::numeric, 6) AS cy,
        ST_Area(geom) AS area,
        geom
    FROM cluster_raw
),
cluster_ordered AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY cx, cy, area DESC) AS cluster_order_id,
        geom
    FROM cluster_ranked
),
cluster_final AS (
    SELECT
        CONCAT('{{ year }}', '-', '{{ urban_threshold }}', '-', LPAD(cluster_order_id::text, 7, '0')) AS cluster_id,
        {{ year }} AS year,
        {{ urban_threshold }} AS urban_threshold,
        geom
    FROM cluster_ordered
)
SELECT * FROM cluster_final
{% endmacro %}
