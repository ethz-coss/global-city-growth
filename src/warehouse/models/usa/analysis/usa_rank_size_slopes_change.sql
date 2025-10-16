WITH usa_rank_size_slopes_base AS (
    SELECT  year AS year_base,
            analysis_id,
            rank_size_slope AS rank_size_slope_base
    FROM {{ source('figure_data_prep', 'usa_rank_size_slopes') }}
    WHERE year IN (1850, 1930)
)
SELECT  year, 
        analysis_id, 
        year_base, 
        rank_size_slope / rank_size_slope_base  - 1 AS rank_size_slope_change
FROM usa_rank_size_slopes_base
JOIN {{ source('figure_data_prep', 'usa_rank_size_slopes') }}
USING (analysis_id)