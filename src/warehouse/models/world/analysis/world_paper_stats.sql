{% set analysis_id = var('constants')['MAIN_ANALYSIS_ID'] %}

-- Share of urban population living in cities above 1M
WITH tot_pop_share_cities_above_1m_1975 AS (
    SELECT  'Share of total population living in cities above 1M in 1975' AS description,
            SUM(total_population_share_cities_above_one_million * population) / SUM(population) AS value
    FROM {{ ref('world_tot_pop_share_cities_above_1m') }}
    WHERE year = 1975
    AND analysis_id = {{ analysis_id }}
),
tot_pop_share_cities_above_1m_2025 AS (
    SELECT  'Share of total population living in cities above 1M in 2025' AS description,
            SUM(total_population_share_cities_above_one_million * population) / SUM(population) AS value
    FROM {{ ref('world_tot_pop_share_cities_above_1m') }}
    WHERE year = 2025
    AND analysis_id = {{ analysis_id }}
),
tot_pop_share_cities_above_1m_2100_inc_returns AS (
    SELECT  'Share of total population living in cities above 1M in 2100 (increasing returns)' AS description,
            SUM(total_population_share_cities_above_one_million * population) / SUM(population) AS value
    FROM {{ ref('world_tot_pop_share_cities_above_1m_projections_inc_returns') }}
    WHERE year = 2100 
    AND analysis_id = {{ analysis_id }}
),
tot_pop_share_cities_above_1m_2100_prop_growth AS (
    SELECT  'Share of total population living in cities above 1M in 2100 (proportional growth)' AS description,
            SUM(total_population_share_cities_above_one_million * population) / SUM(population) AS value
    FROM {{ ref('world_tot_pop_share_cities_above_1m_projections_prop_growth') }}
    WHERE year = 2100 
    AND analysis_id = {{ analysis_id }}
),
tot_pop_share_cities_above_1m_2100_our_model AS (
    SELECT  'Share of total population living in cities above 1M in 2100 (our model)' AS description,
            SUM(total_population_share_cities_above_one_million * population) / SUM(population) AS value
    FROM {{ ref('world_tot_pop_share_cities_above_1m') }}
    WHERE year = 2100 
    AND analysis_id = {{ analysis_id }}
),
tot_pop_share_cities_above_1m_2100_extr AS (
    SELECT  'Share of total population living in cities above 1M in 2100 (extrapolation)' AS description,
            SUM(total_population_share_cities_above_one_million * population) / SUM(population) AS value
    FROM {{ ref('world_tot_pop_share_cities_above_1m_projections_extr') }}
    WHERE year = 2100 
    AND analysis_id = {{ analysis_id }}
),
world_population_2100 AS (
    SELECT  SUM(population) AS total_population
    FROM {{ ref('world_population') }} wp
    JOIN {{ source('cshapes', 'world_crosswalk_cshapes_code_to_iso_code') }} cw
    ON wp.country = cw.world_bank_code
    WHERE year = 2100
),
tot_pop_share_cities_above_1m_2100_diff_ir_pg AS (
    SELECT  'Increasing returns - proportional growth: difference in share of total population living in cities above 1M in 2100' AS description,
            ir.value - pg.value AS value
    FROM tot_pop_share_cities_above_1m_2100_inc_returns ir
    CROSS JOIN tot_pop_share_cities_above_1m_2100_prop_growth pg
),
tot_pop_share_cities_above_1m_2100_diff_ir_pg_pop AS (
    SELECT  'Increasing returns - proportional growth: population difference in cities above 1M in 2100' AS description,
            diff.value * tp.total_population AS value
    FROM tot_pop_share_cities_above_1m_2100_diff_ir_pg diff
    CROSS JOIN world_population_2100 tp
),
tot_pop_share_cities_above_1m_2100_diff_om_ex AS (
    SELECT  'Our model - extrapolation: difference in share of total population living in cities above 1M in 2100' AS description,
            our.value - extr.value AS value
    FROM tot_pop_share_cities_above_1m_2100_our_model our
    CROSS JOIN tot_pop_share_cities_above_1m_2100_extr extr
),
tot_pop_share_cities_above_1m_2100_diff_om_ex_pop AS (
    SELECT  'Our model - extrapolation: population difference in cities above 1M in 2100' AS description,
            diff.value * tp.total_population AS value
    FROM tot_pop_share_cities_above_1m_2100_diff_om_ex diff
    CROSS JOIN world_population_2100 tp
),
tot_pop_share_cities_above_1m_2100_diff_om_pg AS (
    SELECT  'Our model - proportional growth: difference in share of total population living in cities above 1M in 2100' AS description,
            our.value - pg.value AS value
    FROM tot_pop_share_cities_above_1m_2100_our_model our
    CROSS JOIN tot_pop_share_cities_above_1m_2100_prop_growth pg
),
tot_pop_share_cities_above_1m_2100_diff_om_pg_pop AS (
    SELECT  'Our model - proportional growth: population difference in cities above 1M in 2100' AS description,
            diff.value * tp.total_population AS value
    FROM tot_pop_share_cities_above_1m_2100_diff_om_pg diff
    CROSS JOIN world_population_2100 tp
),
-- Size-growth slope
median_size_growth_slope_world_1975_2025 AS (
    SELECT  'Median size growth slope in 1975-2025' AS description,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY size_growth_slope) AS value
    FROM {{ source('world_analysis_python', 'world_size_growth_slopes_historical') }}
    WHERE year >= 1975 AND year <= 2015
    AND analysis_id = {{ analysis_id }}
),
-- Growth of cities above 1M
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
growth_rate_of_cities_above_1m_deameaned_late_urbanizers_1975_2025 AS (
    SELECT  'Growth rate of cities above 1M deameaned late urbanizers in 1975-2025' AS description,
            SUM(normalized_growth * population) / SUM(population) AS value
    FROM {{ ref('world_size_vs_growth_normalized_by_group') }}
    WHERE urban_population_share_group = '0-60'
    AND city_group = 'above_1m'
    AND year >= 1975 AND year <= 2025
    AND analysis_id = {{ analysis_id }}
),
growth_rate_of_cities_above_1m_deameaned_early_urbanizers_1975_2025 AS (
    SELECT  'Growth rate of cities above 1M deameaned early urbanizers in 1975-2025' AS description,
            SUM(normalized_growth * population) / SUM(population) AS value
    FROM {{ ref('world_size_vs_growth_normalized_by_group') }}
    WHERE urban_population_share_group = '60-100'
    AND city_group = 'above_1m'
    AND year >= 1975 AND year <= 2025
    AND analysis_id = {{ analysis_id }}
),
-- Rank size slopes
rank_size_slope_change_late_urbanizers_1975_2025 AS (
    SELECT  'Rank size slope change in late urbanizers in 1975-2025' AS description,
            AVG(rank_size_slope_change) AS value
    FROM {{ ref('world_rank_size_slopes_change_1975_2025') }}
    WHERE urban_population_share_group = '0-60' AND year = 2025
    AND analysis_id = {{ analysis_id }}
),
rank_size_slope_change_early_urbanizers_1975_2025 AS (
    SELECT  'Rank size slope change in early urbanizers in 1975-2025' AS description,
            AVG(rank_size_slope_change) AS value
    FROM {{ ref('world_rank_size_slopes_change_1975_2025') }}
    WHERE urban_population_share_group = '60-100' AND year = 2025
    AND analysis_id = {{ analysis_id }}
),
rank_size_slope_late_urbanizers_1975 AS (
    SELECT  'Rank size slope in late urbanizers in 1975' AS description,
            AVG(rank_size_slope) AS value
    FROM {{ ref('world_rank_size_slopes') }}
    WHERE urban_population_share_group = '0-60' AND year = 1975
    AND analysis_id = {{ analysis_id }}
),
rank_size_slope_early_urbanizers_1975 AS (
    SELECT  'Rank size slope in early urbanizers in 1975' AS description,
            AVG(rank_size_slope) AS value
    FROM {{ ref('world_rank_size_slopes') }}
    WHERE urban_population_share_group = '60-100' AND year = 1975
    AND analysis_id = {{ analysis_id }}
),
rank_size_slope_ratio_late_early_urbanizers_1975 AS (
    SELECT  'Ratio of rank size slope in early urbanizers / late urbanizers in 1975' AS description,
            early.value / late.value - 1 AS value
    FROM rank_size_slope_late_urbanizers_1975 late
    CROSS JOIN rank_size_slope_early_urbanizers_1975 early
),
rank_size_slope_late_urbanizers_2100 AS (
    SELECT  'Rank size slope in late urbanizers in 2100' AS description,
            AVG(rank_size_slope) AS value
    FROM {{ ref('world_rank_size_slopes') }}
    WHERE urban_population_share_group = '0-60' AND year = 2100
    AND analysis_id = {{ analysis_id }}
),
rank_size_slope_early_urbanizers_2100 AS (
    SELECT  'Rank size slope in early urbanizers in 2100' AS description,
            AVG(rank_size_slope) AS value
    FROM {{ ref('world_rank_size_slopes') }}
    WHERE urban_population_share_group = '60-100' AND year = 2100
    AND analysis_id = {{ analysis_id }}
),
rank_size_slope_ratio_late_early_urbanizers_2100 AS (
    SELECT  'Ratio of rank size slope in late urbanizers / early urbanizers in 2100' AS description,
            late.value / early.value - 1 AS value
    FROM rank_size_slope_late_urbanizers_2100 late
    CROSS JOIN rank_size_slope_early_urbanizers_2100 early
),
-- Other stats
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
),
paper_stats AS (
    SELECT * FROM tot_pop_share_cities_above_1m_1975
    UNION ALL
    SELECT * FROM tot_pop_share_cities_above_1m_2025
    UNION ALL
    SELECT * FROM tot_pop_share_cities_above_1m_2100_inc_returns
    UNION ALL
    SELECT * FROM tot_pop_share_cities_above_1m_2100_prop_growth
    UNION ALL
    SELECT * FROM tot_pop_share_cities_above_1m_2100_our_model
    UNION ALL
    SELECT * FROM tot_pop_share_cities_above_1m_2100_extr
    UNION ALL
    SELECT * FROM tot_pop_share_cities_above_1m_2100_diff_ir_pg
    UNION ALL
    SELECT * FROM tot_pop_share_cities_above_1m_2100_diff_ir_pg_pop
    UNION ALL
    SELECT * FROM tot_pop_share_cities_above_1m_2100_diff_om_ex
    UNION ALL
    SELECT * FROM tot_pop_share_cities_above_1m_2100_diff_om_ex_pop
    UNION ALL
    SELECT * FROM tot_pop_share_cities_above_1m_2100_diff_om_pg
    UNION ALL
    SELECT * FROM tot_pop_share_cities_above_1m_2100_diff_om_pg_pop
    UNION ALL
    SELECT * FROM median_size_growth_slope_world_1975_2025
    UNION ALL
    SELECT * FROM growth_rate_of_cities_above_1m_deameaned_africa_1975_2025
    UNION ALL
    SELECT * FROM growth_rate_of_cities_above_1m_deameaned_americas_1975_2025
    UNION ALL
    SELECT * FROM growth_rate_of_cities_above_1m_deameaned_asia_1975_2025
    UNION ALL
    SELECT * FROM growth_rate_of_cities_above_1m_deameaned_late_urbanizers_1975_2025
    UNION ALL
    SELECT * FROM growth_rate_of_cities_above_1m_deameaned_early_urbanizers_1975_2025
    UNION ALL
    SELECT * FROM rank_size_slope_change_late_urbanizers_1975_2025
    UNION ALL
    SELECT * FROM rank_size_slope_change_early_urbanizers_1975_2025
    UNION ALL
    SELECT * FROM rank_size_slope_late_urbanizers_1975
    UNION ALL
    SELECT * FROM rank_size_slope_early_urbanizers_1975
    UNION ALL
    SELECT * FROM rank_size_slope_ratio_late_early_urbanizers_1975
    UNION ALL
    SELECT * FROM rank_size_slope_late_urbanizers_2100
    UNION ALL
    SELECT * FROM rank_size_slope_early_urbanizers_2100
    UNION ALL
    SELECT * FROM rank_size_slope_ratio_late_early_urbanizers_2100
    UNION ALL
    SELECT * FROM world_population_share_in_countries_covered_by_analysis
)
SELECT * FROM paper_stats
