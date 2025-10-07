WITH population_with_left_joined_urbanization AS (
    SELECT  country, 
            year, 
            population, 
            urban_population_share
    FROM {{ ref('world_population') }}
    LEFT JOIN {{ ref('world_urbanization') }}
    USING (country, year)
)
SELECT country, year, population * urban_population_share AS urban_population
FROM population_with_left_joined_urbanization
ORDER BY country, year