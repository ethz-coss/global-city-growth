WITH size_vs_growth_normalized AS (
    SELECT  sg.cluster_id,
            sg.year,
            sg.analysis_id,
            sg.log_population,
            sg.log_growth - ag.log_average_growth AS normalized_log_growth,
            ag.epoch
    FROM {{ ref('usa_size_vs_growth') }} sg
    INNER JOIN {{ ref('usa_average_growth') }} ag
    USING (year, analysis_id)
)
SELECT *
FROM size_vs_growth_normalized