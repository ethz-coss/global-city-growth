WITH years_to_project AS (
    SELECT generate_series(2025, 2075, 5) AS year
),
rank_size_slopes_start AS (
    SELECT  country,
            analysis_id,
            rank_size_slope
    FROM {{ source('world_analysis_python', 'world_rank_size_slopes_historical') }}
    WHERE year = 2025
),
rank_size_slopes_projections AS (
    SELECT  rs.country,
            ytp.year,
            rs.analysis_id,
            rs.rank_size_slope
    FROM rank_size_slopes_start rs
    CROSS JOIN years_to_project ytp
)
SELECT *
FROM rank_size_slopes_projections
ORDER BY analysis_id, country, year