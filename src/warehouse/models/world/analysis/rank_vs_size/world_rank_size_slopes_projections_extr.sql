-- We need to add these otherwise the macro will not work
-- depends_on: {{ source('world_analysis_python', 'world_rank_size_slopes_historical') }}

WITH size_growth_slope_projection_extrapolation AS (
        SELECT  country,
                year + 50 AS country,
                analysis_id,
                AVG(size_growth_slope) AS size_growth_slope
        FROM {{ source('world_analysis_python', 'world_size_growth_slopes_historical') }}
),
rank_size_slopes_projections_extrapolation AS (
    {{ get_rank_size_slopes_projections('size_growth_slope_projection_extrapolation') }}
)
SELECT * FROM rank_size_slopes_projections_extrapolation
ORDER BY analysis_id, country, year