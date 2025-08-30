{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
)}}

{% set years = var('constants')['USA_YEARS'] %}
{% set dbscan_eps = var('constants')['USA_DB_SCAN_EPS'] %}
{% set dbscan_min_points = var('constants')['USA_DB_SCAN_MIN_POINTS'] %}
{% set urban_thresholds = var('constants')['USA_URBAN_POPULATION_PIXEL_THRESHOLDS'] %}


{% set raster_table = source('convolved_raster', 'usa_raster_census_place_convolved_all_years') %}
WITH 
{% for year in years %}
{% for urban_threshold in urban_thresholds %}
    cluster_{{ year }}_{{ urban_threshold }} AS (
        {{ create_clusters_base_from_raster(raster_table, year, urban_threshold, dbscan_eps, dbscan_min_points) }}
    ),
{% endfor %}
{% endfor %}
cluster_union AS (
    {% set queries = [] %}
    {% for year in years %}
    {% for urban_threshold in urban_thresholds %}
        {% set query = "SELECT cluster_id, year, urban_threshold, geom FROM cluster_" ~ year ~ "_" ~ urban_threshold %}
        {% do queries.append(query) %}
    {% endfor %}
    {% endfor %}
    {{ queries | join(' UNION ALL\n') }}
)
SELECT * FROM cluster_union