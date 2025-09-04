{% macro get_usa_cluster_growth_population(urban_threshold, city_population_threshold, analysis_id) %}
    WITH usa_cluster_growth_population AS (
        SELECT  cluster_id, 
                y1, 
                y2,
                population_y1,
                population_y2
        FROM {{ ref('usa_cluster_growth_population') }}
        WHERE population_y1 > {{ city_population_threshold }} AND urban_threshold = {{ urban_threshold }}
    )
    SELECT  cluster_id,
            y1,
            y2,
            population_y1,
            population_y2,
            {{ analysis_id }} AS analysis_id
    FROM usa_cluster_growth_population
{% endmacro %}