--- Here we rename the connected component with the id of the most populous census place in that component
--- Most populous is defined as the census place with the highest sum of population across all years
WITH nhgis_census_place_total_population_over_time AS (
    SELECT census_place_id, SUM(population) AS population
    FROM {{ ref('usa_nhgis_census_place_population') }}
    GROUP BY census_place_id
),
connected_component_and_total_population_over_time AS (
    SELECT component_id, census_place_id, COALESCE(population, 0) AS population
    FROM {{ source('matching', 'usa_crosswalk_nhgis_census_place_to_connected_component') }} 
    LEFT JOIN nhgis_census_place_total_population_over_time
    USING (census_place_id)
),
connected_component_and_most_populous_census_place_over_time AS (
    SELECT component_id, census_place_id AS census_place_stable_id
    FROM ( 
        SELECT component_id, census_place_id, ROW_NUMBER() OVER (PARTITION BY component_id ORDER BY population DESC) AS rank
        FROM connected_component_and_total_population_over_time
    ) 
    WHERE rank = 1
),
crosswalk_nhgis_census_place_to_census_place_stable AS (
    SELECT census_place_stable_id, census_place_id
    FROM {{ source('matching', 'usa_crosswalk_nhgis_census_place_to_connected_component') }} 
    INNER JOIN connected_component_and_most_populous_census_place_over_time
    USING (component_id)
)
SELECT * FROM crosswalk_nhgis_census_place_to_census_place_stable