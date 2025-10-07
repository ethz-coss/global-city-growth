{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
)}}

WITH country_borders_2019 AS (
    SELECT  gwcode,
            ST_CollectionExtract(ST_MakeValid(ST_Transform(ST_UnaryUnion(ST_Collect(geom)), 54009)), 3) AS geom
    FROM {{ ref('world_country_borders') }}
    WHERE gweyear = 2019
    -- Fixe a year in which we want to the country borders and group by country code
    GROUP BY gwcode
)
SELECT * FROM country_borders_2019