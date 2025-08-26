{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
)}}

SELECT  "id" as id,
        ST_Transform(ST_SetSrid(ST_Point(lon, lat), 4326), 5070) AS geom
FROM {{ source('ipums_full_count', 'usa_hist_census_place_geom_raw') }}