{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
)}}


WITH nhgis_census_place_geom_all_years_clean AS (
    SELECT "NHGISPLACE" AS census_place_id,
           "PLACE" AS place_name,
           "geometry" AS geom
    FROM {{ source('nhgis', 'usa_nhgis_census_place_geom_all_years_raw') }}
),
no_duplicates AS (
    SELECT census_place_id, place_name AS census_place_name, geom
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY census_place_id) AS row_number
        FROM nhgis_census_place_geom_all_years_clean
    )
    WHERE row_number = 1
),
transformed AS (
    SELECT census_place_id, census_place_name, ST_Transform(geom, 5070) AS geom
    FROM no_duplicates
)
SELECT * FROM transformed