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
            epoch
    FROM average_growth
    INNER JOIN {{ ref('usa_year_epoch') }}
    USING (year)
)
SELECT * 
FROM average_growth_with_epoch