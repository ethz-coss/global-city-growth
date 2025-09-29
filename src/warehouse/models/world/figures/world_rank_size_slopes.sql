WITH world_rank_size_slopes_with_projections AS (
    SELECT  country, 
            year, 
            analysis_id, 
            rank_size_slope,
            false AS is_projection
    FROM{{ source('figure_data_prep', 'world_rank_size_slopes_historical') }}
    WHERE year < 2030
    UNION ALL
    SELECT  country, 
            year, 
            analysis_id, 
            rank_size_slope,
            true AS is_projection
    FROM {{ ref('world_rank_size_slopes_projections') }}
),
world_rank_size_slopes_with_projections_and_region AS (
    SELECT  country, 
            year, 
            analysis_id, 
            rank_size_slope, 
            is_projection,
            region2 AS region    
    FROM world_rank_size_slopes_with_projections
    JOIN {{ source('owid', 'world_country_region') }}
    USING (country)
)
SELECT *
FROM world_rank_size_slopes_with_projections_and_region
ORDER BY analysis_id, country, year