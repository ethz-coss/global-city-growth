{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
)}}


{% set years = [1900, 1910, 1920, 1930, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010] %}

WITH 
{% for year in years %}
    nhgis_census_place_geom_{{ year }} AS (
        SELECT  "NHGISPLACE" AS census_place_id,
                "PLACE" AS place_name,
                "geometry" AS geom
        FROM {{ source('nhgis', 'usa_nhgis_census_place_geom_' ~ year ~ '_raw') }}
    ),
{% endfor %}
geom_joined AS (
{% for year in years %}
    SELECT * FROM nhgis_census_place_geom_{{ year }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
),
geom_joined_no_duplicates AS (
    SELECT census_place_id, place_name AS census_place_name, geom
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY census_place_id) AS row_number
        FROM geom_joined
    )
    WHERE row_number = 1
),
geom_transformed AS (
    SELECT census_place_id, census_place_name, ST_Transform(geom, 5070) AS geom
    FROM geom_joined_no_duplicates
)
SELECT * FROM geom_transformed