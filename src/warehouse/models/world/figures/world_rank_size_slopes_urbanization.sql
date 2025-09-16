WITH urban_population_share_1950 AS (
        SELECT country, urban_population_share AS urban_population_share_1950
        FROM {{ ref('world_urbanization') }}
        WHERE year = 1950
)
SELECT  country, 
        year, 
        analysis_id,
        rank_size_slope, 
        urban_population_share, 
        region2 AS region,
        urban_population_share_1950
FROM {{ source('figure_data_prep', 'world_rank_size_slopes') }}
JOIN {{ ref('world_urbanization') }}
USING (country, year)
JOIN urban_population_share_1950
USING (country)
JOIN {{ source('owid', 'world_country_region') }}
USING (country)