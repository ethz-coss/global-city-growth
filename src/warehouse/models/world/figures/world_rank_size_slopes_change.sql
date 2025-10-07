WITH world_rank_size_slopes_base AS (
    SELECT  country, 
            year AS year_base,
            analysis_id,
            rank_size_slope AS rank_size_slope_base
    FROM {{ ref('world_rank_size_slopes') }}
    WHERE year IN (1975, 2000, 2025)
)
SELECT  country, 
        year, 
        analysis_id, 
        region, 
        year_base, 
        rank_size_slope / rank_size_slope_base  - 1 AS rank_size_slope_change
FROM world_rank_size_slopes_base
JOIN {{ ref('world_rank_size_slopes') }}
USING (country, analysis_id)