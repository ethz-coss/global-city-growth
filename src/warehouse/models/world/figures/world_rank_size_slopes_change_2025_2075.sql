WITH rank_size_slope_change_with_2025_base AS (
    SELECT *
    FROM {{ ref('world_rank_size_slopes_change') }}
    WHERE year_base = 2025
),
world_rank_size_slopes_change_by_urbanization_group AS (
    SELECT  country, 
            year, 
            year - 2025 AS year_since_2025,
            analysis_id,
            region,
            rank_size_slope_change,
            urban_population_share_group
    FROM rank_size_slope_change_with_2025_base
    JOIN {{ ref('world_urbanization_groups') }}
    USING (country)
    WHERE year >= 2025 AND year <= 2075
)
SELECT *
FROM world_rank_size_slopes_change_by_urbanization_group
ORDER BY analysis_id, country, year