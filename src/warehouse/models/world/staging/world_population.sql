WITH clean_variable_names AS (
    SELECT "Code" AS country, "Year" AS year, "Population (historical)" AS population_historical, "Population (projections)" AS population_projections
    FROM {{ source('owid', 'world_population_raw') }}
),
join_historical_and_projections AS (
    SELECT country, year, population_historical AS population, false AS projection
    FROM clean_variable_names
    WHERE year < 2024
    UNION ALL
    SELECT country, year, population_projections AS population, true AS projection
    FROM clean_variable_names
    WHERE year >= 2024
),
drop_non_country_entities AS (
    SELECT country, year, population, projection
    FROM join_historical_and_projections
    WHERE country IS NOT NULL
),
modify_special_country_names AS (
    SELECT CASE WHEN country = 'OWID_SRM' THEN 'SRB' -- Serbia
                WHEN country = 'ROU' THEN 'ROM' -- Romania
                WHEN country = 'COD' THEN 'ZAR' -- Democratic Republic of the Congo
                ELSE country
            END AS country,
            year,
            population,
            projection
    FROM drop_non_country_entities
)
SELECT * 
FROM modify_special_country_names
ORDER BY country, year