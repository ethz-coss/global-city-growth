SELECT  country, 
        year, 
        size_growth_slope, 
        urban_population_share, 
        takeoff_year,
        analysis_id
FROM {{ source('figure_data_prep', 'world_size_growth_slopes') }}
JOIN {{ ref('world_urbanization') }}
USING (country, year)
JOIN {{ ref('world_takeoff_years') }}
USING (country)