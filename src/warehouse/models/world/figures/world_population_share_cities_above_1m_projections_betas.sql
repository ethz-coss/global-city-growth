WITH candidate_betas AS (
    SELECT generate_series(0.1, 2, 0.1) AS beta
),
urban_population_and_rank_size_slopes AS (
    SELECT  country, 
            year, 
            analysis_id, 
            beta,
            rank_size_slope, 
            urban_population
    FROM {{ ref('world_rank_size_slopes') }}
    JOIN {{ ref('world_urban_population') }}
    USING (country, year)
    CROSS JOIN candidate_betas
),
prep_data AS (
    SELECT  country, 
            year, 
            analysis_id,
            beta,
            1/rank_size_slope AS alpha, 
            beta * urban_population AS x_max,
            LEAST(beta * urban_population, POWER(10, 6)) AS z, 
            5 * POWER(10, 3) AS x_min
    FROM urban_population_and_rank_size_slopes
),
population_share_cities_above_one_million_estimate AS (
    SELECT  country, 
            year, 
            analysis_id,
            beta,
            (POWER(x_max, 1-alpha) - POWER(z, 1-alpha)) / (POWER(x_max, 1-alpha) - POWER(x_min, 1-alpha)) AS population_share_cities_above_one_million_estimate
    FROM prep_data
),
population_share_cities_above_one_million_real AS (
    SELECT  country, 
            year, 
            analysis_id, 
            SUM(CASE WHEN log_population >= 6 THEN POWER(10, log_population) ELSE 0 END) / SUM(POWER(10, log_population)) AS population_share_cities_above_one_million_real
    FROM world_rank_vs_size
    GROUP BY country, year, analysis_id
),
distance_estimate_real AS (
    SELECT  country, 
            analysis_id,
            beta,
            MAX(ABS(population_share_cities_above_one_million_estimate - population_share_cities_above_one_million_real)) AS distance
    FROM population_share_cities_above_one_million_estimate
    JOIN population_share_cities_above_one_million_real
    USING (country, year, analysis_id)
    GROUP BY country, analysis_id, beta
),
ranked_betas AS (
    SELECT  country, 
            analysis_id, 
            beta, 
            distance,
            ROW_NUMBER() OVER (PARTITION BY country, analysis_id ORDER BY distance) AS rank
    FROM distance_estimate_real
)
SELECT *
FROM ranked_betas
WHERE rank = 1
ORDER BY analysis_id, country, beta