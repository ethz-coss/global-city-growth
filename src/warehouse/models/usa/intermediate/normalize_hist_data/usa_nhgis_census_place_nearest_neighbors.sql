-- Find the nearest neighbors for each nhgis census place
SELECT  cp1.census_place_id AS census_place_id,
        nbhr.census_place_id AS neighbor_census_place_id,
        nbhr.distance
FROM {{ ref('usa_nhgis_census_place_geom') }} cp1
CROSS JOIN LATERAL (
    SELECT cp2.census_place_id, cp1.geom <-> cp2.geom AS distance
    FROM {{ ref('usa_nhgis_census_place_geom') }} cp2
    ORDER BY distance
    LIMIT 100
) nbhr