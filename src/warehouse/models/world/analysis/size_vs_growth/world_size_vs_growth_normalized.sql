WITH size_vs_growth_normalized AS (
    SELECT  sg.cluster_id,
            sg.country, 
            sg.year,
            sg.analysis_id,
            sg.log_population,
            sg.log_growth - ag.log_average_growth AS normalized_log_growth,
            ag.region
    FROM {{ ref('world_size_vs_growth') }} sg
    INNER JOIN {{ ref('world_average_growth') }} ag
    USING (country, year, analysis_id)
)
SELECT *
FROM size_vs_growth_normalized