{% set analysis_id = 1 %}

WITH share_of_urban_population_living_in_cities_above_1m_world_1975 AS (
    SELECT  'Share of urban population living in cities above 1M in 1975' AS description,
            SUM(urban_population * population_share_cities_above_one_million) / SUM(urban_population) AS value
    FROM {{ ref('world_population_share_cities_above_1m') }}
    WHERE year = 1975 
    AND analysis_id = {{ analysis_id }}
),
share_of_urban_population_living_in_cities_above_1m_world_2025 AS (
    SELECT  'Share of urban population living in cities above 1M in 2025' AS description,
            SUM(urban_population * population_share_cities_above_one_million) / SUM(urban_population) AS value
    FROM {{ ref('world_population_share_cities_above_1m') }}
    WHERE year = 2025 
    AND analysis_id = {{ analysis_id }}
),
share_of_urban_population_living_in_cities_above_1m_world_2060 AS (
    SELECT  'Share of urban population living in cities above 1M in 2060' AS description,
            SUM(urban_population * population_share_cities_above_one_million) / SUM(urban_population) AS value
    FROM {{ ref('world_population_share_cities_above_1m') }}
    WHERE year = 2060 
    AND analysis_id = {{ analysis_id }}
),
share_of_urban_population_living_in_cities_above_1m_africa_2060 AS (
    SELECT  'Share of urban population living in cities above 1M in Africa in 2060' AS description,
            SUM(urban_population * population_share_cities_above_one_million) / SUM(urban_population) AS value
    FROM {{ ref('world_population_share_cities_above_1m') }}
    WHERE year = 2060 
    AND region = 'Africa'
    AND analysis_id = {{ analysis_id }}
),
share_of_urban_population_living_in_cities_above_1m_americas_2060 AS (
    SELECT  'Share of urban population living in cities above 1M in Americas in 2060' AS description,
            SUM(urban_population * population_share_cities_above_one_million) / SUM(urban_population) AS value
    FROM {{ ref('world_population_share_cities_above_1m') }}
    WHERE year = 2060 
    AND region = 'Americas'
    AND analysis_id = {{ analysis_id }}
),
share_of_urban_population_living_in_cities_above_1m_asia_2060 AS (
    SELECT  'Share of urban population living in cities above 1M in Asia in 2060' AS description,
            SUM(urban_population * population_share_cities_above_one_million) / SUM(urban_population) AS value
    FROM {{ ref('world_population_share_cities_above_1m') }}
    WHERE year = 2060 
    AND region = 'Asia'
    AND analysis_id = {{ analysis_id }}
),
share_of_urban_population_living_in_cities_above_1m_europe_2060 AS (
    SELECT  'Share of urban population living in cities above 1M in Europe in 2060' AS description,
            SUM(urban_population * population_share_cities_above_one_million) / SUM(urban_population) AS value
    FROM {{ ref('world_population_share_cities_above_1m') }}
    WHERE year = 2060 
    AND region = 'Europe'
    AND analysis_id = {{ analysis_id }}
),
average_growth_rate_of_chinese_cities_of_less_than_1m_1975_2025 AS (
    SELECT  'Average growth rate of Chinese cities of less than 1M in 1975-2025' AS description,
            POWER(10, AVG(log_average_growth_group)) AS value
    FROM {{ ref('world_average_growth_group') }}
    WHERE country = 'CHN'
    AND "group" = 'below_1M'
    AND year >= 1975 AND year <= 2015
    AND analysis_id = {{ analysis_id }}
),
total_population_of_chinese_cities_above_1m_1975 AS (
    SELECT  'Total population of Chinese cities above 1M in 1975' AS description,
            SUM(urban_population) AS value
    FROM {{ ref('world_population_share_cities_above_1m') }}
    WHERE country = 'CHN'
    AND year = 1975
    AND analysis_id = {{ analysis_id }}
),
total_population_of_chinese_cities_above_1m_2025 AS (
    SELECT  'Total population of Chinese cities above 1M in 2025' AS description,
            SUM(urban_population * population_share_cities_above_one_million) AS value
    FROM {{ ref('world_population_share_cities_above_1m') }}
    WHERE country = 'CHN'
    AND year = 2025
    AND analysis_id = {{ analysis_id }}
),
total_population_of_chinese_cities_above_1m_2025_with_below_1m_growth_rate AS (
    SELECT  'Total population of Chinese cities above 1M in 2025 with below 1M growth rate' AS description,
            POWER(gr.value, 5) * pop.value AS value
    FROM average_growth_rate_of_chinese_cities_of_less_than_1m_1975_2025 gr
    CROSS JOIN total_population_of_chinese_cities_above_1m_1975 pop
),
large_city_concentration_growth_rate_world_1975_2025 AS (
    SELECT  'Large city concentration growth rate in 1975-2025' AS description,
            AVG(rank_size_slope_decade_change) AS value
    FROM {{ ref('world_rank_size_slopes_decade_change') }}
    WHERE year >= 1975 AND year <= 2015
    AND analysis_id = {{ analysis_id }}
),
large_city_concentration_growth_rate_world_2025_2060 AS (
    SELECT  'Large city concentration growth rate in 2025-2060' AS description,
            AVG(rank_size_slope_decade_change) AS value
    FROM {{ ref('world_rank_size_slopes_decade_change') }}
    WHERE year >= 2025 AND year <= 2060
    AND analysis_id = {{ analysis_id }}
),
large_city_concentration_growth_rate_world_2050_2060 AS (
    SELECT  'Large city concentration growth rate in 2050-2060' AS description,
            AVG(rank_size_slope_decade_change) AS value
    FROM {{ ref('world_rank_size_slopes_decade_change') }}
    WHERE year >= 2050 AND year <= 2060
    AND analysis_id = {{ analysis_id }}
),
large_city_concentration_growth_rate_asia_1975_2025 AS (
    SELECT  'Large city concentration growth rate in Asia in 1975-2025' AS description,
            AVG(rank_size_slope_decade_change) AS value
    FROM {{ ref('world_rank_size_slopes_decade_change') }}
    WHERE region = 'Asia'
    AND year >= 1975 AND year <= 2015
    AND analysis_id = {{ analysis_id }}
),
large_city_concentration_growth_rate_asia_2025_2060 AS (
    SELECT  'Large city concentration growth rate in Asia in 2025-2060' AS description,
            AVG(rank_size_slope_decade_change) AS value
    FROM {{ ref('world_rank_size_slopes_decade_change') }}
    WHERE region = 'Asia'
    AND year >= 2025 AND year <= 2060
    AND analysis_id = {{ analysis_id }}
),
large_city_concentration_growth_rate_asia_2050_2060 AS (
    SELECT  'Large city concentration growth rate in Asia in 2050-2060' AS description,
            AVG(rank_size_slope_decade_change) AS value
    FROM {{ ref('world_rank_size_slopes_decade_change') }}
    WHERE region = 'Asia'
    AND year >= 2050 AND year <= 2060
    AND analysis_id = {{ analysis_id }}
),
large_city_concentration_growth_rate_africa_1975_2025 AS (
    SELECT  'Large city concentration growth rate in Africa in 1975-2025' AS description,
            AVG(rank_size_slope_decade_change) AS value
    FROM {{ ref('world_rank_size_slopes_decade_change') }}
    WHERE region = 'Africa'
    AND year >= 1975 AND year <= 2015
    AND analysis_id = {{ analysis_id }}
),
large_city_concentration_growth_rate_africa_2025_2060 AS (
    SELECT  'Large city concentration growth rate in Africa in 2025-2060' AS description,
            AVG(rank_size_slope_decade_change) AS value
    FROM {{ ref('world_rank_size_slopes_decade_change') }}
    WHERE region = 'Africa'
    AND year >= 2025 AND year <= 2060
    AND analysis_id = {{ analysis_id }}
),
large_city_concentration_growth_rate_africa_2050_2060 AS (
    SELECT  'Large city concentration growth rate in Africa in 2050-2060' AS description,
            AVG(rank_size_slope_decade_change) AS value
    FROM {{ ref('world_rank_size_slopes_decade_change') }}
    WHERE region = 'Africa'
    AND year >= 2050 AND year <= 2060
    AND analysis_id = {{ analysis_id }}
),
large_city_concentration_growth_rate_europe_2050_2060 AS (
    SELECT  'Large city concentration growth rate in Europe in 2050-2060' AS description,
            AVG(rank_size_slope_decade_change) AS value
    FROM {{ ref('world_rank_size_slopes_decade_change') }}
    WHERE region = 'Europe'
    AND year >= 2050 AND year <= 2060
    AND analysis_id = {{ analysis_id }}
),
large_city_concentration_growth_rates_americas_2050_2060 AS (
    SELECT  'Large city concentration growth rate in Americas in 2050-2060' AS description,
            AVG(rank_size_slope_decade_change) AS value
    FROM {{ ref('world_rank_size_slopes_decade_change') }}
    WHERE region = 'Americas'
    AND year >= 2050 AND year <= 2060
    AND analysis_id = {{ analysis_id }}
),
median_size_growth_slope_world_1975_2025 AS (
    SELECT  'Median size growth slope in 1975-2025' AS description,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY size_growth_slope) AS value
    FROM {{ source('figure_data_prep', 'world_size_growth_slopes_historical') }}
    WHERE year >= 1975 AND year <= 2015
    AND analysis_id = {{ analysis_id }}
),
growth_rate_of_cities_above_1m_deameaned_africa_1975_2025 AS (
    SELECT  'Growth rate of cities above 1M deameaned Africa in 1975-2025' AS description,
            POWER(10, AVG(log_average_growth_group_demeaned)) AS value
    FROM {{ ref('world_average_growth_group') }}
    WHERE region = 'Africa'
    AND "group" = 'above_1M'
    AND year >= 1975 AND year <= 2025
    AND analysis_id = {{ analysis_id }}
),
growth_rate_of_cities_above_1m_deameaned_americas_1975_2025 AS (
    SELECT  'Growth rate of cities above 1M deameaned Americas in 1975-2025' AS description,
            POWER(10, AVG(log_average_growth_group_demeaned)) AS value
    FROM {{ ref('world_average_growth_group') }}
    WHERE region = 'Americas'
    AND "group" = 'above_1M'
    AND year >= 1975 AND year <= 2025
    AND analysis_id = {{ analysis_id }}
),
growth_rate_of_cities_above_1m_deameaned_asia_1975_2025 AS (
    SELECT  'Growth rate of cities above 1M deameaned Asia in 1975-2025' AS description,
            POWER(10, AVG(log_average_growth_group_demeaned)) AS value
    FROM {{ ref('world_average_growth_group') }}
    WHERE region = 'Asia' 
    AND "group" = 'above_1M'
    AND year >= 1975 AND year <= 2025
    AND analysis_id = {{ analysis_id }}
),
paper_stats AS (
    SELECT * FROM share_of_urban_population_living_in_cities_above_1m_world_1975
    UNION ALL
    SELECT * FROM share_of_urban_population_living_in_cities_above_1m_world_2025
    UNION ALL
    SELECT * FROM share_of_urban_population_living_in_cities_above_1m_world_2060
    UNION ALL
    SELECT * FROM share_of_urban_population_living_in_cities_above_1m_africa_2060
    UNION ALL
    SELECT * FROM share_of_urban_population_living_in_cities_above_1m_americas_2060
    UNION ALL
    SELECT * FROM share_of_urban_population_living_in_cities_above_1m_asia_2060
    UNION ALL
    SELECT * FROM share_of_urban_population_living_in_cities_above_1m_europe_2060
    UNION ALL
    SELECT * FROM average_growth_rate_of_chinese_cities_of_less_than_1m_1975_2025
    UNION ALL
    SELECT * FROM total_population_of_chinese_cities_above_1m_1975
    UNION ALL
    SELECT * FROM total_population_of_chinese_cities_above_1m_2025
    UNION ALL
    SELECT * FROM total_population_of_chinese_cities_above_1m_2025_with_below_1m_growth_rate
    UNION ALL
    SELECT * FROM large_city_concentration_growth_rate_world_1975_2025
    UNION ALL
    SELECT * FROM large_city_concentration_growth_rate_world_2025_2060
    UNION ALL
    SELECT * FROM large_city_concentration_growth_rate_world_2050_2060
    UNION ALL
    SELECT * FROM large_city_concentration_growth_rate_asia_1975_2025
    UNION ALL
    SELECT * FROM large_city_concentration_growth_rate_asia_2025_2060
    UNION ALL
    SELECT * FROM large_city_concentration_growth_rate_asia_2050_2060
    UNION ALL
    SELECT * FROM large_city_concentration_growth_rate_africa_1975_2025
    UNION ALL
    SELECT * FROM large_city_concentration_growth_rate_africa_2025_2060
    UNION ALL
    SELECT * FROM large_city_concentration_growth_rate_africa_2050_2060
    UNION ALL
    SELECT * FROM large_city_concentration_growth_rate_europe_2050_2060
    UNION ALL
    SELECT * FROM large_city_concentration_growth_rates_americas_2050_2060  
    UNION ALL
    SELECT * FROM median_size_growth_slope_world_1975_2025
    UNION ALL
    SELECT * FROM growth_rate_of_cities_above_1m_deameaned_africa_1975_2025
    UNION ALL
    SELECT * FROM growth_rate_of_cities_above_1m_deameaned_americas_1975_2025
    UNION ALL
    SELECT * FROM growth_rate_of_cities_above_1m_deameaned_asia_1975_2025
)
SELECT * FROM paper_stats
