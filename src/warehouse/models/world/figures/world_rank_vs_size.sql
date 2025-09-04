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

{% for row in analysis_parameters %}
    {{ get_world_rank_vs_size(
                urban_threshold=row.world_urban_threshold,
                city_population_threshold=row.world_city_population_threshold,
                country_min_num_cities=row.world_country_min_num_cities,
                country_exclude=row.world_country_exclude,
                analysis_id=row.analysis_id
            ) 
    }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}