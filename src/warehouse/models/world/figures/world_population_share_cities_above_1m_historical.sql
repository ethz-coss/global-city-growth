SELECT  country, 
        year, 
        analysis_id, 
        ANY_VALUE(region2) AS region,
        ANY_VALUE(urban_population) AS urban_population,
        SUM(CASE WHEN log_population >= 6 THEN POWER(10, log_population) ELSE 0 END) / SUM(POWER(10, log_population)) AS population_share_cities_above_one_million
FROM {{ ref('world_rank_vs_size') }}
JOIN {{ source('owid', 'world_country_region') }}
USING (country)
JOIN {{ ref('world_urban_population') }}
USING (country, year)
GROUP BY country, year, analysis_id