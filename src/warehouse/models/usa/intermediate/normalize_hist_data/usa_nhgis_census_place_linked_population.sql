-- Calculate the 'linked' population for each nhgis census place
-- The linked population of a census place in a given census years contains all the people who have been linked
-- This differs from the population of a census place in a given census year, which contains also people who have not been linked
WITH nhgis_census_place_linked_population AS (
    SELECT  census_place_origin AS census_place_id,
            year_origin AS year,
            SUM(all_migrants) AS population
    FROM {{ ref('usa_nhgis_census_place_migration') }}
    GROUP BY census_place_origin, year_origin
)
SELECT * FROM nhgis_census_place_linked_population