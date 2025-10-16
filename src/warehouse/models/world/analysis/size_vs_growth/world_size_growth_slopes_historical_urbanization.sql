SELECT  country, 
        year, 
        analysis_id,
        size_growth_slope, 
        urban_population_share
FROM {{ source('figure_data_prep', 'world_size_growth_slopes_historical') }}
JOIN {{ ref('world_urbanization') }}
USING (country, year)