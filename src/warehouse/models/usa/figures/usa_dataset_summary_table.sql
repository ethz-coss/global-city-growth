SELECT  analysis_id, 
        COUNT(DISTINCT cluster_id) AS num_cities, 
        1 AS num_countries, 
        COUNT(DISTINCT y1) + 1 AS num_years, 
        MIN(y1) AS min_year, 
        MAX(y2) AS max_year
FROM {{ ref('usa_cluster_growth_population_analysis') }}
GROUP BY analysis_id