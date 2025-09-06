-- depends_on: {{ ref('world_cluster_growth_population_country') }}
{% set get_analysis_parameters_query %}
    SELECT
        analysis_id,
        world_urban_threshold,
        world_city_population_threshold,
        world_country_min_num_cities,
        world_country_exclude
    FROM {{ source('other', 'analysis_parameters') }}
{% endset %}

{% set analysis_parameters = run_query(get_analysis_parameters_query) %}

WITH
{% for row in analysis_parameters %}
    world_cluster_growth_population_country_analysis_{{ row.analysis_id }} AS (
        {{ get_world_cluster_growth_population_country(
                    urban_threshold=row.world_urban_threshold,
                    city_population_threshold=row.world_city_population_threshold,
                    country_min_num_cities=row.world_country_min_num_cities,
                    country_exclude=row.world_country_exclude,
                    analysis_id=row.analysis_id
                ) 
        }}
    ),
{% endfor %}
world_cluster_growth_population_country_analysis AS (
    {% set queries = [] %}
    {% for row in analysis_parameters %}
    {% set query = "SELECT * FROM world_cluster_growth_population_country_analysis_" ~ row.analysis_id %}
    {% do queries.append(query) %}
    {% endfor %}
    {{ queries | join(' UNION ALL\n') }}
)
SELECT * FROM world_cluster_growth_population_country_analysis