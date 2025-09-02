{% set population_threshold = var('constants')['WORLD_CITY_POPULATION_THRESHOLD'] %}
{% set urban_threshold = var('defaults')['WORLD_DEGREE_OF_URBANIZATION_THRESHOLD'] %}

SELECT  cluster_id,
        y1 AS year,
        country,
        LOG(population_y1) AS log_population,
        LOG(ROW_NUMBER() OVER (PARTITION BY y1, country ORDER BY population_y1 DESC)) AS log_city_rank
FROM {{ ref('world_cluster_growth_population_country') }}
WHERE y1 = y2 AND population_y1 > {{ population_threshold }} AND urban_threshold = {{ urban_threshold }}