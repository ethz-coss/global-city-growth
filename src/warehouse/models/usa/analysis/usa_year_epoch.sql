WITH years AS (
    SELECT DISTINCT y1 AS year
    FROM {{ ref('usa_cluster_growth_population_analysis') }}
),
epochs AS (
    SELECT  year,
            CASE WHEN year < 1900 THEN '1850-1880'
                 WHEN year < 1950 THEN '1900-1940'
                 ELSE '1990-2020'
            END AS epoch
    FROM years
)
SELECT * FROM epochs