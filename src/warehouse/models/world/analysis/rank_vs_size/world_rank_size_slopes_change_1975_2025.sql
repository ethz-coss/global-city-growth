WITH rank_size_slope_change_with_1975_base AS (
    SELECT *
    FROM {{ ref('world_rank_size_slopes_change') }}
    WHERE year_base = 1975
),
world_rank_size_slopes_change_by_urbanization_group AS (
    SELECT  country, 
            year, 
            year - 1975 AS year_since_1975,
            analysis_id,
            region,
            rank_size_slope_change,
            urban_population_share_group
    FROM rank_size_slope_change_with_1975_base
    JOIN {{ ref('world_urbanization_groups') }}
    USING (country)
    WHERE year >= 1975 AND year <= 2025
)
SELECT *
FROM world_rank_size_slopes_change_by_urbanization_group
ORDER BY analysis_id, country, year