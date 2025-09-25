WITH rank_size_slopes_decade_change AS (
SELECT  country, 
        year, 
        analysis_id, 
        region, 
        LEAD(rank_size_slope) OVER (PARTITION BY country, analysis_id, MOD(year, 10) ORDER BY year) / rank_size_slope - 1 AS rank_size_slope_decade_change
FROM {{ ref('world_rank_size_slopes') }}
)
SELECT *
FROM rank_size_slopes_decade_change
WHERE rank_size_slope_decade_change IS NOT NULL