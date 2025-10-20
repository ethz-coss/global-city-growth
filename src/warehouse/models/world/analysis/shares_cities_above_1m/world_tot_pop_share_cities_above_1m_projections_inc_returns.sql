-- We need to add these otherwise the macro will not work
-- depends_on: {{ ref('world_urban_population') }}
-- depends_on: {{ source('owid', 'world_country_region') }}
-- depends_on: {{ ref('world_urb_pop_share_cities_above_1m_historical') }}
-- depends_on: {{ source('world_analysis_python', 'world_rank_size_slopes_historical') }}
WITH size_growth_slope_projection_inc_returns AS (
    WITH years_to_project AS (
        SELECT generate_series(2020, 2090, 10) AS year
    ),
    size_growth_slopes_projection AS (
        SELECT 0.03 AS size_growth_slope
    ),
    countries_analysis_ids AS (
        SELECT DISTINCT country, analysis_id
        FROM {{ source('world_analysis_python', 'world_rank_size_slopes_historical') }}
    )
    SELECT country, 
           year,
           analysis_id,
           size_growth_slope
    FROM size_growth_slopes_projection
    CROSS JOIN years_to_project ytp
    CROSS JOIN countries_analysis_ids
),
rank_size_slopes_projections_inc_returns AS (
    {{ get_rank_size_slopes_projections('size_growth_slope_projection_inc_returns') }}
),
rank_size_slopes_inc_returns AS (
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
    FROM rank_size_slopes_projections_inc_returns
),
urban_population_share_in_cities_above_1m_inc_returns_projections AS (
    {{ get_population_share_cities_above_1m_projections('rank_size_slopes_inc_returns') }}
),
total_population_share_in_cities_above_1m_inc_returns_projections AS (
    SELECT  country, 
            year, 
            analysis_id, 
            urban_population_share_cities_above_one_million * urban_population_share AS 
            total_population_share_cities_above_one_million,
            population
    FROM urban_population_share_in_cities_above_1m_inc_returns_projections
    JOIN {{ ref('world_urbanization') }}
    USING (country, year)
    JOIN {{ ref('world_population') }}
    USING (country, year)
    WHERE year >= 2025
)
SELECT *
FROM total_population_share_in_cities_above_1m_inc_returns_projections