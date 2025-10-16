WITH urbanization_groups AS (
    SELECT  country, 
            CASE WHEN urban_population_share <= 0.6 THEN '0-60'
                 ELSE '60-100'
            END AS urban_population_share_group
    FROM {{ ref('world_urbanization') }}
    WHERE year = 1975
)
SELECT *
FROM urbanization_groups