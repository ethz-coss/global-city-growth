WITH usa_urbanization AS (
    SELECT year, urban_population_share
    FROM {{ ref('world_urbanization') }}
    WHERE country = 'USA'
)
SELECT  year, 
        analysis_id,
        rank_size_slope, 
        urban_population_share
FROM {{ source('figure_data_prep', 'usa_rank_size_slopes') }}
JOIN usa_urbanization
USING (year)