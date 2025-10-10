{% set analysis_id = 1 %}

WITH share_of_urban_population_living_in_cities_above_1m_world_1975 AS (
    SELECT  'Share of urban population living in cities above 1M in 1975' AS description,
            AVG(urban_population_share_cities_above_one_million) AS value
    FROM {{ ref('world_population_share_cities_above_1m') }}
    WHERE year = 1975
    AND analysis_id = {{ analysis_id }}
),
share_of_urban_population_living_in_cities_above_1m_world_2025 AS (
    SELECT  'Share of urban population living in cities above 1M in 2025' AS description,
            AVG(urban_population_share_cities_above_one_million) AS value
    FROM {{ ref('world_population_share_cities_above_1m') }}
    WHERE year = 2025
    AND analysis_id = {{ analysis_id }}
),
share_of_urban_population_living_in_cities_above_1m_world_2075 AS (
    SELECT  'Share of urban population living in cities above 1M in 2075' AS description,
            AVG(urban_population_share_cities_above_one_million) AS value
    FROM {{ ref('world_population_share_cities_above_1m') }}
    WHERE year = 2075 
    AND analysis_id = {{ analysis_id }}
),
share_of_urban_population_living_in_cities_above_1m_early_2075 AS (
    SELECT  'Share of urban population living in cities above 1M in early 2075' AS description,
            AVG(urban_population_share_cities_above_one_million) AS value
    FROM {{ ref('world_population_share_cities_above_1m') }}
    WHERE year = 2075 AND urban_population_share_group = '0-60'
    AND analysis_id = {{ analysis_id }}
),
share_of_urban_population_living_in_cities_above_1m_late_2075 AS (
    SELECT  'Share of urban population living in cities above 1M in late 2075' AS description,
            AVG(urban_population_share_cities_above_one_million) AS value
    FROM {{ ref('world_population_share_cities_above_1m') }}
    WHERE year = 2075 AND urban_population_share_group = '60-100'
    AND analysis_id = {{ analysis_id }}
),
average_growth_rate_of_chinese_cities_below_1m_1975_2025 AS (
    SELECT  'Average growth rate of Chinese cities of less than 1M in 1975-2025' AS description,
            SUM(POWER(10, log_growth) * POWER(10, log_population)) / SUM(POWER(10, log_population)) AS value
    FROM {{ ref('world_size_vs_growth') }}
    WHERE country = 'CHN'
    AND log_population < 6
    AND year >= 1975 AND year <= 2015
    AND analysis_id = {{ analysis_id }}
),
average_growth_rate_of_chinese_cities_above_1m_1975_2025 AS (
    SELECT  'Average growth rate of Chinese cities of above 1M in 1975-2025' AS description,
            SUM(POWER(10, log_growth) * POWER(10, log_population)) / SUM(POWER(10, log_population)) AS value
    FROM {{ ref('world_size_vs_growth') }}
    WHERE country = 'CHN'
    AND log_population >= 6
    AND year >= 1975 AND year <= 2015
    AND analysis_id = {{ analysis_id }}
),
large_city_concentration_growth_rate_world_late_1975_2025 AS (
    SELECT  'Large city concentration growth rate in 1975-2025' AS description,
            AVG(rank_size_slope_change) AS value
    FROM {{ ref('world_rank_size_slopes_change_1975_2025') }}
    WHERE year = 2025 AND urban_population_share_group = '0-60'
    AND analysis_id = {{ analysis_id }}
),
large_city_concentration_growth_rate_world_late_2025_2075 AS (
    SELECT  'Large city concentration growth rate in 2025-2075' AS description,
            AVG(rank_size_slope_change) AS value
    FROM {{ ref('world_rank_size_slopes_change_2025_2075') }}
    WHERE year = 2075 AND urban_population_share_group = '0-60'
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
            SUM(normalized_growth * population) / SUM(population) AS value
    FROM {{ ref('world_size_vs_growth_normalized_by_group') }}
    WHERE region = 'Africa'
    AND city_group = 'above_1m'
    AND year >= 1975 AND year <= 2025
    AND analysis_id = {{ analysis_id }}
),
growth_rate_of_cities_above_1m_deameaned_americas_1975_2025 AS (
    SELECT  'Growth rate of cities above 1M deameaned Americas in 1975-2025' AS description,
            SUM(normalized_growth * population) / SUM(population) AS value
    FROM {{ ref('world_size_vs_growth_normalized_by_group') }}
    WHERE region = 'Americas'
    AND city_group = 'above_1m'
    AND year >= 1975 AND year <= 2025
    AND analysis_id = {{ analysis_id }}
),
growth_rate_of_cities_above_1m_deameaned_asia_1975_2025 AS (
    SELECT  'Growth rate of cities above 1M deameaned Asia in 1975-2025' AS description,
            SUM(normalized_growth * population) / SUM(population) AS value
    FROM {{ ref('world_size_vs_growth_normalized_by_group') }}
    WHERE region = 'Asia' 
    AND city_group = 'above_1m'
    AND year >= 1975 AND year <= 2025
    AND analysis_id = {{ analysis_id }}
),
average_rank_size_slope_late_2075 AS (
    SELECT  'Average rank size slope in late 2075' AS description,
            AVG(rank_size_slope) AS value
    FROM {{ ref('world_rank_size_slopes') }}
    WHERE year = 2075 AND urban_population_share_group = '0-60'
    AND analysis_id = {{ analysis_id }}
),
average_rank_size_slope_early_2075 AS (
    SELECT  'Average rank size slope in early 2075' AS description,
            AVG(rank_size_slope) AS value
    FROM {{ ref('world_rank_size_slopes') }}
    WHERE year = 2075 AND urban_population_share_group = '60-100'
    AND analysis_id = {{ analysis_id }}
),
world_population_share_in_countries_covered_by_analysis AS (
    WITH countries_in_analysis AS (
        SELECT DISTINCT country
        FROM {{ ref('world_cluster_growth_population_country_analysis') }}
        WHERE analysis_id = {{ analysis_id }}
    ),
    total_population_countries_in_analysis AS (
        SELECT  SUM(population) AS total_population_analysis
        FROM {{ ref('world_population') }}
        WHERE year = 2025
        AND country IN (SELECT country FROM countries_in_analysis)
    ),
    total_world_population_2025 AS (
        SELECT  SUM(population) AS total_population
        FROM {{ ref('world_population') }} wp
        JOIN {{ source('cshapes', 'world_crosswalk_cshapes_code_to_iso_code') }} cw
        ON wp.country = cw.world_bank_code
        WHERE year = 2025
    )
    SELECT  'World population share in countries covered by analysis in 2025' AS description,
            total_population_analysis / total_population AS value
    FROM total_population_countries_in_analysis
    CROSS JOIN total_world_population_2025
)
paper_stats AS (
    SELECT * FROM share_of_urban_population_living_in_cities_above_1m_world_1975
    UNION ALL
    SELECT * FROM share_of_urban_population_living_in_cities_above_1m_world_2025
    UNION ALL
    SELECT * FROM share_of_urban_population_living_in_cities_above_1m_world_2075
    UNION ALL
    SELECT * FROM share_of_urban_population_living_in_cities_above_1m_early_2075
    UNION ALL
    SELECT * FROM share_of_urban_population_living_in_cities_above_1m_late_2075
    UNION ALL
    SELECT * FROM average_growth_rate_of_chinese_cities_below_1m_1975_2025
    UNION ALL
    SELECT * FROM average_growth_rate_of_chinese_cities_above_1m_1975_2025
    UNION ALL
    SELECT * FROM large_city_concentration_growth_rate_world_late_1975_2025
    UNION ALL
    SELECT * FROM large_city_concentration_growth_rate_world_late_2025_2075
    UNION ALL
    SELECT * FROM median_size_growth_slope_world_1975_2025
    UNION ALL
    SELECT * FROM growth_rate_of_cities_above_1m_deameaned_africa_1975_2025
    UNION ALL
    SELECT * FROM growth_rate_of_cities_above_1m_deameaned_americas_1975_2025
    UNION ALL
    SELECT * FROM growth_rate_of_cities_above_1m_deameaned_asia_1975_2025
    UNION ALL
    SELECT * FROM average_rank_size_slope_early_2075
    UNION ALL
    SELECT * FROM average_rank_size_slope_late_2075
    UNION ALL
    SELECT * FROM world_population_share_in_countries_covered_by_analysis
)
SELECT * FROM paper_stats
