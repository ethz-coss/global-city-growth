SELECT  cluster_id,
        y1 AS year,
        LOG(population_y1) AS log_population,
        LOG(population_y2 / population_y1) AS log_growth,
        analysis_id
FROM {{ ref('usa_cluster_growth_population_analysis') }}
WHERE y2 = y1 + 10