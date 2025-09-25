-- The equation to predict the population share of cities above one million in 2060 is:
-- s = (x_max^{1-alpha} - z^{1-alpha}) / (x_max^{1-alpha} - x_min^{1-alpha})
-- Here x_max is the urban population in 2060, z is 1 million, x_min is 5 thousand, and alpha is the zipf exponent (1 / rank_size_slope)
WITH urban_population_and_rank_size_slopes AS (
    SELECT  country, 
            year, 
            analysis_id, 
            rank_size_slope, 
            urban_population
    FROM {{ ref('world_urban_population') }}
    JOIN {{ ref('world_rank_size_slopes') }}
    USING (country, year)
),
prep_data AS (
    SELECT  country, 
            year, 
            analysis_id, 
            urban_population,
            1/rank_size_slope AS alpha, 
            urban_population AS x_max, 
            LEAST(urban_population - 1, POWER(10, 6)) AS z, 
            5 * POWER(10, 3) AS x_min
    FROM urban_population_and_rank_size_slopes
),
population_share_cities_above_one_million AS (
    SELECT  country, 
            year, 
            analysis_id,
            urban_population,
            (POWER(x_max, 1-alpha) - POWER(z, 1-alpha)) / (POWER(x_max, 1-alpha) - POWER(x_min, 1-alpha)) AS population_share_cities_above_one_million
    FROM prep_data
),
population_share_cities_above_one_million_with_region AS (
    SELECT  country, 
            year, 
            analysis_id, 
            urban_population,
            population_share_cities_above_one_million, 
            region2 AS region
    FROM population_share_cities_above_one_million
    JOIN {{ source('owid', 'world_country_region') }}
    USING (country)
)
SELECT *
FROM population_share_cities_above_one_million_with_region
ORDER BY analysis_id, country, year