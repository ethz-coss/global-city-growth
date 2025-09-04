-- depends_on: {{ ref('usa_cluster_growth_population') }}
{% set get_analysis_parameters_query %}
    SELECT
        analysis_id,
        usa_urban_threshold,
        usa_city_population_threshold
    FROM {{ source('other', 'analysis_parameters') }}
{% endset %}

{% set analysis_parameters = run_query(get_analysis_parameters_query) %}

{% for row in analysis_parameters %}
    {{ get_usa_cluster_growth_population(
                urban_threshold=row.usa_urban_threshold,
                city_population_threshold=row.usa_city_population_threshold,
                analysis_id=row.analysis_id
            ) 
    }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}