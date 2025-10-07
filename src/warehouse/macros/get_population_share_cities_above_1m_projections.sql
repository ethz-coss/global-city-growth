{% macro get_population_share_cities_above_1m_projections(rank_size_slope_table) %}
WITH optimal_omegas AS (
    {{ get_optimal_omegas_population_share_cities_above_1m_projections(rank_size_slope_table) }}
),
urban_population_and_rank_size_slopes AS (
    SELECT  country, 
            year, 
            analysis_id,
            omega,
            rank_size_slope, 
            urban_population
    FROM {{ ref('world_urban_population') }}
    JOIN {{ rank_size_slope_table }}
    USING (country, year)
    JOIN optimal_omegas
    USING (country, analysis_id)
),
prep_data AS (
    SELECT  country, 
            year, 
            analysis_id, 
            1/rank_size_slope AS alpha, 
            omega * urban_population AS x_max, 
            LEAST(omega * urban_population, POWER(10, 6)) AS z, 
            5 * POWER(10, 3) AS x_min
    FROM urban_population_and_rank_size_slopes
),
pop_share_cities_above_one_million AS (
    SELECT  country, 
            year, 
            analysis_id,
            (POWER(x_max, 1-alpha) - POWER(z, 1-alpha)) / (POWER(x_max, 1-alpha) - POWER(x_min, 1-alpha)) AS urban_population_share_cities_above_one_million
    FROM prep_data
)
SELECT *
FROM pop_share_cities_above_one_million
ORDER BY analysis_id, country, year
{% endmacro %}