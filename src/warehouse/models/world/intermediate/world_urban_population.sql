WITH population_with_left_joined_urbanization AS (
    SELECT  country, 
            year, 
            population, 
            urban_population_share
    FROM {{ ref('world_population') }}
    LEFT JOIN {{ ref('world_urbanization') }}
    USING (country, year)
),
marks AS (
  SELECT country, 
         year, 
         population, 
         urban_population_share,
         SUM(CASE WHEN urban_population_share IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY country ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM population_with_left_joined_urbanization
),
forward_fill_urban_population_share AS (
  SELECT  country,
          year,
          population,
          urban_population_share,
          MAX(urban_population_share) OVER (PARTITION BY country, grp) AS urban_population_share_filled
  FROM marks
)
SELECT country, year, population * urban_population_share_filled AS urban_population
FROM forward_fill_urban_population_share
WHERE year <= 2060
ORDER BY country, year