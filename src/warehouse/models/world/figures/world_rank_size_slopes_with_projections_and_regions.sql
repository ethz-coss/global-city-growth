WITH world_rank_size_slopes_year_mod10 AS (
    SELECT country, year, analysis_id, rank_size_slope
    FROM {{ source('figure_data_prep', 'world_rank_size_slopes') }}
    WHERE year < 2020 AND MOD(year, 10) = 0
),
world_rank_size_slopes_with_projections AS (
    SELECT country, year, analysis_id, rank_size_slope
    FROM world_rank_size_slopes_year_mod10
    UNION ALL
    SELECT country, year, analysis_id, rank_size_slope
    FROM {{ ref('world_rank_size_slopes_projections') }}
),
world_rank_size_slopes_with_projections_and_region AS (
    SELECT country, year, analysis_id, rank_size_slope, region2 AS region    
    FROM world_rank_size_slopes_with_projections
    JOIN {{ source('owid', 'world_country_region') }}
    USING (country)
)
SELECT *
FROM world_rank_size_slopes_with_projections_and_region
ORDER BY analysis_id, country, year