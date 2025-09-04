WITH city_population AS (
    SELECT  cluster_id,
            y1 AS year,
            analysis_id,
            population_y1,
            population_y2
    FROM {{ ref('usa_cluster_growth_population_analysis') }}
    WHERE y1 + 10 = y2
),
average_growth AS (
    SELECT  year, 
            analysis_id,
            LOG(SUM(population_y2) / SUM(population_y1)) AS log_average_growth
    FROM city_population
    GROUP BY year, analysis_id 
),
average_growth_with_epoch AS (
    SELECT  year, 
            analysis_id, 
            log_average_growth, 
            CASE WHEN year < 1900 THEN '1850-1880'
                 WHEN year < 1950 THEN '1900-1940'
                 ELSE '1990-2020'
            END AS epoch
    FROM average_growth
)
SELECT * 
FROM average_growth_with_epoch