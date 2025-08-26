WITH pop_1990_2020 AS (
    SELECT  "GISJOIN" AS gisjoin,
            "DATAYEAR" AS year,
            "CL8AA" AS population
    FROM {{ source('nhgis', 'usa_nhgis_census_place_population_1990_2020_raw') }}
),
nhgis_census_place_2010 AS (
    SELECT  "GISJOIN" AS gisjoin,
            "NHGISPLACE" AS census_place_id
    FROM {{ source('nhgis', 'usa_nhgis_census_place_geom_all_years_raw') }}
    WHERE "YEAR" = 2010
),
--- From 1990 to 2020 the census offers the population standardized to 2010 census places. This is why we fix the year to 2010 below.
nhgis_census_place_pop_1990_2020 AS (
    SELECT  census_place_id,
            year,
            population
    FROM pop_1990_2020 
    INNER JOIN nhgis_census_place_2010 
    USING (gisjoin)
)
SELECT * FROM nhgis_census_place_pop_1990_2020
