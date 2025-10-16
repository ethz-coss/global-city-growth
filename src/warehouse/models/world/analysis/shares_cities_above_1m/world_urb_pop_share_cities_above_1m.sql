WITH population_share_cities_above_1m_union_historical_and_projections AS (  
    SELECT  country, 
            year, 
            analysis_id,
            urban_population_share_cities_above_one_million,
            false AS is_projection
    FROM {{ ref('world_urb_pop_share_cities_above_1m_historical') }}
    WHERE year <= 2025
    UNION ALL
    SELECT  country, 
            year, 
            analysis_id, 
            urban_population_share_cities_above_one_million,
            true AS is_projection
    FROM {{ ref('world_urb_pop_share_cities_above_1m_projections') }}
    WHERE year > 2025
),
population_share_cities_above_1m_and_urbanization_group AS (
    SELECT  country,
            year,
            analysis_id,
            urban_population_share_cities_above_one_million,
            urban_population,
            urban_population_share_group
    FROM population_share_cities_above_1m_union_historical_and_projections
    JOIN {{ ref('world_urban_population') }}
    USING (country, year)
    JOIN {{ ref('world_urbanization_groups') }}
    USING (country)
)
SELECT *
FROM population_share_cities_above_1m_and_urbanization_group
ORDER BY analysis_id, country, year