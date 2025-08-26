-- The migration probability between two census places is the ratio of the number of migrants to the linked population of the origin census place
-- Infact migrants are part of the linked population, so we need to divide by the linked population (and not the population)
SELECT  census_place_origin, 
        census_place_destination,
        year_origin,
        year_destination,
        all_migrants / population AS migration_probability
FROM {{ ref('usa_nhgis_census_place_migration') }} m
JOIN {{ ref('usa_nhgis_census_place_linked_population') }} lp
ON m.census_place_origin = lp.census_place_id AND m.year_origin = lp.year