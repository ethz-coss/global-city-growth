{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
)}}


{% set years = var('constants')['GHSL_RASTER_YEARS'] %}
{% set dbscan_eps = var('constants')['WORLD_DB_SCAN_EPS'] %}
{% set dbscan_minpoints = var('constants')['WORLD_DB_SCAN_MIN_POINTS'] %}
{% set degree_of_urbanization_thresholds = var('constants')['WORLD_DEGREE_OF_URBANIZATION_THRESHOLDS'] %}


{% set smod_table = source('ghsl', 'world_raster_ghsl_smod_all_years') %}
WITH
{% for urban_threshold in degree_of_urbanization_thresholds %}
{% for year in years %}
    cluster_{{ year }}_{{ urban_threshold }} AS (
        {{ create_clusters_base_from_raster(smod_table, year, urban_threshold, dbscan_eps, dbscan_minpoints) }}
    ),
{% endfor %}
{% endfor %}
cluster_union AS (
    {% set queries = [] %}
    {% for year in years %}
    {% for urban_threshold in degree_of_urbanization_thresholds %}
        {% set query = "SELECT cluster_id, year, urban_threshold, geom FROM cluster_" ~ year ~ "_" ~ urban_threshold %}
        {% do queries.append(query) %}
    {% endfor %}
    {% endfor %}
    {{ queries | join(' UNION ALL\n') }}
)
SELECT * FROM cluster_union