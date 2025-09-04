WITH city_population AS (
    SELECT  cluster_id,
            y1 AS year,
            country, 
            analysis_id,
            population_y1,
            population_y2
    FROM {{ ref('world_cluster_growth_population_country_analysis') }}
    WHERE y1 + 10 = y2
),
average_growth AS (
    SELECT  country, 
            year, 
            analysis_id,
            LOG(SUM(population_y2) / SUM(population_y1)) AS log_average_growth
    FROM city_population
    GROUP BY country, year, analysis_id 
),
average_growth_with_region AS (
    SELECT  country, 
            year, 
            analysis_id, 
            log_average_growth, 
            region2 AS region
    FROM average_growth
    INNER JOIN {{ source('owid', 'countries_with_regions') }}
    USING (country)
)
SELECT * 
FROM average_growth_with_region