-- We need to add these otherwise the macro will not work
-- depends_on: {{ ref('world_urban_population') }}
-- depends_on: {{ source('owid', 'world_country_region') }}
-- depends_on: {{ ref('world_urb_pop_share_cities_above_1m_historical') }}
-- depends_on: {{ source('world_analysis_python', 'world_rank_size_slopes_historical') }}
WITH size_growth_slope_projection_extrapolation AS (
    WITH years_to_project AS (
        SELECT generate_series(2020, 2090, 10) AS year
    ),
    average_size_growth_slope_historical AS (
        SELECT  country,
                analysis_id,
                AVG(size_growth_slope) AS size_growth_slope
        FROM {{ source('world_analysis_python', 'world_size_growth_slopes_historical') }}
        GROUP BY country, analysis_id
    )
    SELECT country, 
           year,
           analysis_id,
           size_growth_slope
    FROM average_size_growth_slope_historical
    CROSS JOIN years_to_project ytp
),
rank_size_slopes_projections_extrapolation AS (
    {{ get_rank_size_slopes_projections('size_growth_slope_projection_extrapolation') }}
),
rank_size_slopes_extrapolation AS (
    SELECT  country, 
            year, 
            analysis_id, 
            rank_size_slope
    FROM {{ source('world_analysis_python', 'world_rank_size_slopes_historical') }}
    WHERE year < 2030
    UNION ALL
    SELECT  country, 
            year, 
            analysis_id, 
            rank_size_slope
    FROM rank_size_slopes_projections_extrapolation
),
urban_population_share_in_cities_above_1m_extrapolation_projections AS (
    {{ get_population_share_cities_above_1m_projections('rank_size_slopes_extrapolation') }}
),
total_population_share_in_cities_above_1m_extrapolation_projections AS (
    SELECT  country, 
            year, 
            analysis_id, 
            urban_population_share_cities_above_one_million * urban_population_share AS 
            total_population_share_cities_above_one_million,
            population
    FROM urban_population_share_in_cities_above_1m_extrapolation_projections
    JOIN {{ ref('world_urbanization') }}
    USING (country, year)
    JOIN {{ ref('world_population') }}
    USING (country, year)
    WHERE year >= 2025
)
SELECT *
FROM total_population_share_in_cities_above_1m_extrapolation_projections