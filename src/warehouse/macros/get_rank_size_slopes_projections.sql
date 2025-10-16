{% macro get_rank_size_slopes_projections(size_growth_slope_projections_table) %}

WITH rank_size_slope_change AS (
    SELECT  country, 
            year AS y1,
            year + 10 AS y2,
            analysis_id,
            size_growth_slope,
            EXP(SUM(LN(1 + size_growth_slope)) OVER (PARTITION BY country, analysis_id ORDER BY year)) - 1 AS rank_size_slope_change
    FROM {{ size_growth_slope_projections_table }}
    WHERE MOD(year, 10) = 5
),
rank_size_slopes_start AS (
    SELECT  country,
            year,
            analysis_id,
            rank_size_slope AS rank_size_slope_start
    FROM {{ source('world_analysis_python', 'world_rank_size_slopes_historical') }}
    WHERE year = 2025
),
rank_size_slopes_projections AS (
    SELECT  country, 
            y2 AS year,
            analysis_id,
            rank_size_slope_start + rank_size_slope_change AS rank_size_slope
    FROM rank_size_slopes_start
    JOIN rank_size_slope_change
    USING (country, analysis_id)
)
SELECT * 
FROM rank_size_slopes_projections
ORDER BY analysis_id, country, year
{% endmacro %}