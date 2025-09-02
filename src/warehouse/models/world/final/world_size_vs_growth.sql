{% set population_threshold = var('constants')['WORLD_CITY_POPULATION_THRESHOLD'] %}
{% set urban_threshold = var('defaults')['WORLD_DEGREE_OF_URBANIZATION_THRESHOLD'] %}

WITH log_size_vs_growth AS (
    SELECT  cluster_id,
            y1 AS year,
            country,
            LOG(population_y1) AS log_population_y1,
            LOG(population_y2 / population_y1) AS log_growth
    FROM {{ ref('world_cluster_growth_population_country') }}
    WHERE y2 = y1 + 10 AND population_y1 > {{ population_threshold }} AND urban_threshold = {{ urban_threshold }}
)
SELECT * FROM log_size_vs_growth