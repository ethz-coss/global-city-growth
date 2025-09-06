SELECT  country, 
        year, 
        rank_size_slope, 
        urban_population_share, 
        takeoff_year,
        region2 AS region,
        analysis_id
FROM {{ source('figure_data_prep', 'world_rank_size_slopes') }}
JOIN {{ ref('world_urbanization') }}
USING (country, year)
JOIN {{ ref('world_takeoff_years') }}
USING (country)
JOIN {{ source('owid', 'world_country_region') }}
USING (country)