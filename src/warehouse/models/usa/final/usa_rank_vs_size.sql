{% set population_threshold = var('constants')['USA_CITY_POPULATION_THRESHOLD'] %}
{% set urban_threshold = 50 %}

WITH log_rank_vs_size AS (
    SELECT  cluster_id,
            y1 AS year,
            LOG(population_y1) AS log_population,
            LOG(ROW_NUMBER() OVER (PARTITION BY y1 ORDER BY population_y1 DESC)) AS log_city_rank
    FROM {{ ref('usa_cluster_growth_population') }}
    WHERE y1 = y2 AND population_y1 > {{ population_threshold }} AND urban_threshold = {{ urban_threshold }}
)
SELECT *
FROM log_rank_vs_size