--- The population of census place between 1850 and 1940 + the population of census place between 1990 and 2020
SELECT census_place_id, year, population
FROM {{ ref('usa_census_place_stable_population_interpolated') }}
UNION ALL
SELECT census_place_id, year, population
FROM {{ ref('usa_nhgis_census_place_population_1990_2020') }}