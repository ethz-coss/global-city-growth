SELECT  cluster_id,
        y1 AS year,
        country,
        LOG(population_y1) AS log_population,
        LOG(population_y2 / population_y1) AS log_growth,
        analysis_id
FROM {{ ref('world_cluster_growth_population_country_analysis') }}
WHERE y2 = y1 + 10