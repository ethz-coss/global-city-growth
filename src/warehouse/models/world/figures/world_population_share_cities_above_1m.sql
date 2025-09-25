WITH world_population_share_cities_above_1m_union_historical_and_projections AS (
    SELECT  country, 
            year, 
            analysis_id, 
            region,
            urban_population,
            population_share_cities_above_one_million,
            true AS is_projection
    FROM {{ ref('world_population_share_cities_above_1m_projections') }}
    WHERE year > 2025
    UNION ALL
    SELECT  country, 
            year, 
            analysis_id,
            region,
            urban_population,
            population_share_cities_above_one_million,
            false AS is_projection
    FROM {{ ref('world_population_share_cities_above_1m_historical') }}
    WHERE year <= 2025
)
SELECT *
FROM world_population_share_cities_above_1m_union_historical_and_projections
ORDER BY analysis_id, country, year