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
modify_special_country_names AS (
    SELECT CASE WHEN country = 'OWID_SRM' THEN 'SRB' -- Serbia
                WHEN country = 'ROU' THEN 'ROM' -- Romania
                WHEN country = 'COD' THEN 'ZAR' -- Democratic Republic of the Congo
                ELSE country
            END AS country,
            year,
            urban_population_share
    FROM drop_non_country_entities
),
divide_share_by_100 AS (
    SELECT country, year, urban_population_share / 100 AS urban_population_share
    FROM modify_special_country_names
)
SELECT * FROM divide_share_by_100