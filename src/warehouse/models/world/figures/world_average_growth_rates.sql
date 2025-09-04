WITH city_population AS (
    SELECT  cluster_id,
            y1 AS year,
            country, 
            analysis_id,
            population_y1,
            population_y2
    FROM {{ ref('world_cluster_growth_population_country_analysis') }}
    WHERE y1 + 10 = y2
),
average_growth_rate AS (
    SELECT  country, 
            year, 
            analysis_id,
            LOG(SUM(population_y2) / SUM(population_y1)) AS log_average_growth_rate
    FROM city_population
    GROUP BY country, year, analysis_id 
)
SELECT * 
FROM average_growth_rate