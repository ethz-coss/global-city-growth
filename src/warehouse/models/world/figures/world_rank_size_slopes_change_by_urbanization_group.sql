WITH urbanization_groups AS (
    SELECT  country, 
            CASE WHEN urban_population_share <= 0.6 THEN '0-60'
                 ELSE '60-100'
            END AS urban_population_share_group
    FROM {{ ref('world_urbanization') }}
    WHERE year = 1975
),
rank_size_slope_change_with_1975_base AS (
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
    JOIN urbanization_groups
    USING (country)
)
SELECT *
FROM world_rank_size_slopes_change_by_urbanization_group
ORDER BY analysis_id, country, year