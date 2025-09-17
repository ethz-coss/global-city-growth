WITH usa_rank_size_slopes_base AS (
    SELECT  year AS year_base,
            analysis_id,
            rank_size_slope AS rank_size_slope_base
    FROM {{ ref('usa_rank_size_slopes_urbanization') }}
    WHERE year IN (1975, 2000)
)
SELECT year, analysis_id, region, urban_population_share, year_base, rank_size_slope / rank_size_slope_base  - 1 AS rank_size_slope_change
FROM usa_rank_size_slopes_base
JOIN {{ ref('usa_rank_size_slopes_urbanization') }}
USING (analysis_id)