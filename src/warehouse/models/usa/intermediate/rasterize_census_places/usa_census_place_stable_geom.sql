{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
)}}

WITH census_place_stable_ids AS (
    SELECT DISTINCT census_place_id
    FROM {{ ref('usa_census_place_stable_population') }}
),
census_place_stable_geom AS (
    SELECT census_place_id, geom
    FROM {{ ref('usa_nhgis_census_place_geom') }}
    WHERE census_place_id IN (SELECT census_place_id FROM census_place_stable_ids)
)
SELECT * FROM census_place_stable_geom