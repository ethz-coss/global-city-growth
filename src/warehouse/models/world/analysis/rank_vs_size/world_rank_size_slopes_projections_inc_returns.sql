WITH years_to_project AS (
    SELECT generate_series(2020, 2065, 5) AS year
),
size_growth_slopes_projection AS (
    SELECT 0.02 AS size_growth_slope
),
rank_size_slope_change AS (
    SELECT year AS y1,
           year + 10 AS y2,
           size_growth_slope,
           MOD(year, 10) AS mod_year,
           EXP(SUM(LN(1 + size_growth_slope)) OVER (PARTITION BY MOD(year, 10) ORDER BY year)) - 1 AS rank_size_slope_change
    FROM size_growth_slopes_projection
    CROSS JOIN years_to_project ytp
),
rank_size_slopes_start AS (
    SELECT  country,
            year,
            analysis_id,
            MOD(year, 10) AS mod_year,
            rank_size_slope AS rank_size_slope_start
    FROM {{ source('world_analysis_python', 'world_rank_size_slopes_historical') }}
    WHERE year IN (2020, 2025)
),
rank_size_slopes_projections AS (
    SELECT  country,
            y2 AS year,
            analysis_id,
            rank_size_slope_start + rank_size_slope_change AS rank_size_slope
    FROM rank_size_slopes_start
    JOIN rank_size_slope_change
    USING (mod_year)
)
SELECT *
FROM rank_size_slopes_projections
ORDER BY analysis_id, country, year

