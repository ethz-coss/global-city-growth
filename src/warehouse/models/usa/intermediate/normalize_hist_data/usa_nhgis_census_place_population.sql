-- Population of nhgis census places aggregated from hist census place population
WITH nhgis_census_place_population AS (
    SELECT  cw.nhgis_census_place_id AS census_place_id, 
            hcpp.year, 
            SUM(hcpp.population) AS population
    FROM {{ source('ipums_full_count', 'usa_hist_census_place_population') }} hcpp
    INNER JOIN {{ ref('usa_crosswalk_hist_census_place_to_nhgis_census_place') }} cw
    ON hcpp.census_place_id = cw.hist_census_place_id
    GROUP BY cw.nhgis_census_place_id, hcpp.year
)
SELECT * FROM nhgis_census_place_population