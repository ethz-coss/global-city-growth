-- depends_on: {{ ref('usa_cluster_growth_population') }}
{% set get_analysis_parameters_query %}
    SELECT
        analysis_id,
        usa_urban_threshold,
        usa_city_population_threshold
    FROM {{ source('other', 'analysis_parameters') }}
{% endset %}

{% set analysis_parameters = run_query(get_analysis_parameters_query) %}

WITH
{% for row in analysis_parameters %}
    usa_cluster_growth_population_analysis_{{ row.analysis_id }} AS (
        {{ get_usa_cluster_growth_population(
                urban_threshold=row.usa_urban_threshold,
                city_population_threshold=row.usa_city_population_threshold,
                analysis_id=row.analysis_id
            ) 
    }}
    ),
{% endfor %}
usa_cluster_growth_population_analysis AS (
    {% set queries = [] %}
    {% for row in analysis_parameters %}
    {% set query = "SELECT * FROM usa_cluster_growth_population_analysis_" ~ row.analysis_id %}
    {% do queries.append(query) %}
    {% endfor %}
    {{ queries | join(' UNION ALL\n') }}
)
SELECT * FROM usa_cluster_growth_population_analysis