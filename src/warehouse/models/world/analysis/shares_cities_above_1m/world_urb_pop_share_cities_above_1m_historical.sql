WITH urban_population_share_cities_above_1m AS (
        SELECT  country, 
                year, 
                analysis_id, 
                SUM(CASE WHEN log_population >= 6 THEN POWER(10, log_population) ELSE 0 END) / SUM(POWER(10, log_population)) AS urban_population_share_cities_above_one_million
        FROM {{ ref('world_rank_vs_size') }}
        GROUP BY country, year, analysis_id
)
SELECT *
FROM urban_population_share_cities_above_1m