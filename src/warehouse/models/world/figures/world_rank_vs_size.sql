SELECT  cluster_id,
        y1 AS year,
        country,
        LOG(population_y1) AS log_population,
        LOG(ROW_NUMBER() OVER (PARTITION BY y1, country, analysis_id ORDER BY population_y1 DESC)) AS log_rank,
        analysis_id
FROM {{ ref('world_cluster_growth_population_country_analysis') }}
WHERE y1 = y2