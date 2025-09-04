SELECT  cluster_id,
        y1 AS year,
        LOG(population_y1) AS log_population,
        LOG(ROW_NUMBER() OVER (PARTITION BY y1 ORDER BY population_y1 DESC)) AS log_rank,
        analysis_id
FROM {{ ref('usa_cluster_growth_population_analysis') }}
WHERE y1 = y2