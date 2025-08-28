{% set population_threshold = var('constants')['USA_CITY_POPULATION_THRESHOLD'] %}
{% set pixel_threshold = var('defaults')['USA_PIXEL_THRESHOLD'] %}

WITH log_size_vs_growth AS (
    SELECT  cluster_id,
            y1 AS year,
            LOG(population_y1) AS log_population_y1,
            LOG(population_y2 / population_y1) AS log_growth
    FROM {{ ref('usa_cluster_growth_population') }}
    WHERE y2 = y1 + 10 AND population_y1 > {{ population_threshold }} AND pixel_threshold = {{ pixel_threshold }}
)
SELECT * FROM log_size_vs_growth