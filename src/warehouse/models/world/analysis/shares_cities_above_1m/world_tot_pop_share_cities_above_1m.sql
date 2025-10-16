SELECT  country, 
        year, 
        analysis_id,
        urban_population_share_cities_above_one_million * urban_population_share AS total_population_share_cities_above_one_million,
        population
FROM {{ ref('world_urb_pop_share_cities_above_1m') }}
JOIN {{ ref('world_urbanization') }}
USING (country, year)
JOIN {{ ref('world_population') }}
USING (country, year)