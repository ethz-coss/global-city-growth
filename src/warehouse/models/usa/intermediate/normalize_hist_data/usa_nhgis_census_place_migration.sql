-- Migration flows between nhgis census places aggregated from hist census place migration
WITH nhgis_census_place_migration AS (
    SELECT  cwo.nhgis_census_place_id AS census_place_origin, 
            cwd.nhgis_census_place_id AS census_place_destination,
            hcm.year_origin AS year_origin, 
            hcm.year_destination AS year_destination, 
            SUM(hcm.all_migrants) AS all_migrants
    FROM {{ source('ipums_full_count', 'usa_hist_census_place_migration')}} hcm
    INNER JOIN {{ ref('usa_crosswalk_hist_census_place_to_nhgis_census_place') }} cwo
    ON hcm.census_place_origin = cwo.hist_census_place_id
    INNER JOIN {{ ref('usa_crosswalk_hist_census_place_to_nhgis_census_place') }} cwd
    ON hcm.census_place_destination = cwd.hist_census_place_id
    GROUP BY cwo.nhgis_census_place_id, cwd.nhgis_census_place_id, hcm.year_origin, hcm.year_destination
)
SELECT * FROM nhgis_census_place_migration