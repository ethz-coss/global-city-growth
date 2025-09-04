WITH country_year_ranked_by_proximity_to_urban_pct_x AS (
    SELECT  country, 
            year, 
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY ABS(urban_population_share - 0.2)) AS rank
    FROM {{ ref('world_urbanization') }}
    WHERE year >= 1700
)
SELECT country, year AS takeoff_year
FROM country_year_ranked_by_proximity_to_urban_pct_x
WHERE rank = 1