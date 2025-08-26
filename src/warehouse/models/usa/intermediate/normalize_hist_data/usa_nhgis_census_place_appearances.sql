-- Number of times a census place appears in the data and the last year it appears in the data
-- This is used to identify census places that were present in the data but disappeared in later years
SELECT  census_place_id, 
        COUNT(*) AS num_appearances, 
        MAX(year) AS last_appearance
FROM {{ ref('usa_nhgis_census_place_population') }}
GROUP BY census_place_id 