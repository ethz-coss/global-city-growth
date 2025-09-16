WITH rank_size_slope_change AS (
    SELECT  country, 
            year AS y1,
            year + 10 AS y2,
            analysis_id,
            size_growth_slope,
            SUM(size_growth_slope) OVER (PARTITION BY country, analysis_id ORDER BY year) AS rank_size_slope_change
    FROM {{ source('figure_data_prep', 'world_size_growth_slopes_projections') }}
),
rank_size_slopes_2020 AS (
    SELECT  country,
            analysis_id,
            rank_size_slope AS rank_size_slope_2020
    FROM {{ source('figure_data_prep', 'world_rank_size_slopes') }}
    WHERE year = 2020
),
rank_size_slopes_projections AS (
    SELECT  country, 
            y2 AS year,
            analysis_id,
            rank_size_slope_2020 + rank_size_slope_change AS rank_size_slope
    FROM rank_size_slopes_2020
    JOIN rank_size_slope_change
    USING (country, analysis_id)
)
SELECT * FROM rank_size_slopes_projections
