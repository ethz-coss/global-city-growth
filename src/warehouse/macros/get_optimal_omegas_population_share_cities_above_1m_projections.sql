{% macro get_optimal_omegas_population_share_cities_above_1m_projections(rank_size_slope_table) %}

WITH candidate_omegas AS (
    SELECT generate_series(0.1, 2, 0.1) AS omega
),
urban_population_and_rank_size_slopes AS (
    SELECT  country, 
            year, 
            analysis_id, 
            omega,
            rank_size_slope, 
            urban_population
    FROM {{ rank_size_slope_table }}
    JOIN {{ ref('world_urban_population') }}
    USING (country, year)
    CROSS JOIN candidate_omegas
),
prep_data AS (
    SELECT  country, 
            year, 
            analysis_id,
            omega,
            1/rank_size_slope AS alpha, 
            omega * urban_population AS x_max,
            LEAST(omega * urban_population, POWER(10, 6)) AS z, 
            5 * POWER(10, 3) AS x_min
    FROM urban_population_and_rank_size_slopes
),
urban_population_share_cities_above_one_million_estimate AS (
    SELECT  country, 
            year, 
            analysis_id,
            omega,
            (POWER(x_max, 1-alpha) - POWER(z, 1-alpha)) / (POWER(x_max, 1-alpha) - POWER(x_min, 1-alpha)) AS urban_population_share_cities_above_one_million_estimate
    FROM prep_data
),
urban_population_share_cities_above_one_million_real AS (
    SELECT  country, 
            year, 
            analysis_id,
            urban_population_share_cities_above_one_million AS urban_population_share_cities_above_one_million_real
    FROM {{ ref('world_urb_pop_share_cities_above_1m_historical') }}
    CROSS JOIN candidate_omegas
),
distance_estimate_real AS (
    SELECT  country, 
            analysis_id,
            omega,
            AVG(ABS(urban_population_share_cities_above_one_million_estimate - urban_population_share_cities_above_one_million_real)) AS distance
    FROM urban_population_share_cities_above_one_million_estimate
    JOIN urban_population_share_cities_above_one_million_real
    USING (country, year, analysis_id)
    GROUP BY country, analysis_id, omega
),
ranked_omegas AS (
    SELECT  country, 
            analysis_id, 
            omega, 
            distance,
            ROW_NUMBER() OVER (PARTITION BY country, analysis_id ORDER BY distance) AS rank
    FROM distance_estimate_real
)
SELECT *
FROM ranked_omegas
WHERE rank = 1
ORDER BY analysis_id, country, omega

{% endmacro %}