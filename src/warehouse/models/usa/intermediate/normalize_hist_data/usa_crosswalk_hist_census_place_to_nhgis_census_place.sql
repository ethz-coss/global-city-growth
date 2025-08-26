-- Link hist census place to its closest nhgis census place
SELECT  hcpg.id AS hist_census_place_id, 
        ncpg.census_place_id AS nhgis_census_place_id, 
        ncpg.distance
FROM {{ ref('usa_hist_census_place_geom') }} hcpg
CROSS JOIN LATERAL (
    SELECT ncpg.census_place_id, hcpg.geom <-> ncpg.geom AS distance
    FROM {{ ref('usa_nhgis_census_place_geom') }} ncpg
    ORDER BY distance
    LIMIT 1
) ncpg