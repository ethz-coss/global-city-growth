WITH clean_variable_names AS (
    SELECT  "Code" AS country,
            "Year" AS year,
            "Share of population residing in urban areas (HYDE estimates and" AS urban_population_share
    FROM {{ source('owid', 'world_urbanization_raw') }}
),
drop_non_country_entities AS (
    SELECT country, year, urban_population_share
    FROM clean_variable_names
    WHERE country IS NOT NULL
),
divide_share_by_100 AS (
    SELECT country, year, urban_population_share / 100 AS urban_population_share
    FROM drop_non_country_entities
)
SELECT * FROM divide_share_by_100