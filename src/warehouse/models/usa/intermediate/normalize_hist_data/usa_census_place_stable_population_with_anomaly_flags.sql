WITH census_place_stable_population AS (
    SELECT census_place_stable_id AS census_place_id, year, SUM(population) AS population
    FROM {{ ref('usa_nhgis_census_place_population') }}
    JOIN {{ ref('usa_crosswalk_nhgis_census_place_to_census_place_stable') }}
    USING (census_place_id)
    GROUP BY census_place_stable_id, year
),
census_place_stable_appearences AS (
    SELECT census_place_id, MAX(year) AS last_appearance, MIN(year) AS first_appearance
    FROM census_place_stable_population
    GROUP BY census_place_id
),
census_place_stable_population_with_first_appearance AS (
    SELECT census_place_id, year, population, first_appearance
    FROM census_place_stable_population
    JOIN census_place_stable_appearences
    USING (census_place_id)
),
census_place_stable_growth AS(
    SELECT  census_place_id, 
            year, 
            population,
            first_appearance,
            LEAD(population) OVER (PARTITION BY census_place_id ORDER BY year) / population AS population_growth_p1,
            LEAD(population, 2) OVER (PARTITION BY census_place_id ORDER BY year) / population AS population_growth_p2,
            population / LAG(population) OVER (PARTITION BY census_place_id ORDER BY year) AS population_growth_m1,
            population / LAG(population, 2) OVER (PARTITION BY census_place_id ORDER BY year) AS population_growth_m2
    FROM census_place_stable_population_with_first_appearance
),
census_place_stable_population_with_anomaly_flags AS (
    SELECT  census_place_id,
            year,
            population,
            CASE WHEN (population_growth_p1 > 2.0 AND population_growth_m1 < 0.5) THEN 1
                 WHEN (population_growth_p1 < 0.5 AND population_growth_m1 > 2.0) THEN 1
                 WHEN (population > 100000 AND population_growth_p1 > 1.5 AND population_growth_m1 < 0.75) THEN 1
                 WHEN (population > 100000 AND population_growth_p1 < 0.75 AND population_growth_m1 > 1.5) THEN 1
                 WHEN (population_growth_p2 > 4.0 AND population_growth_m1 < 0.5) THEN 1
                 WHEN (population_growth_p1 > 2.0 AND population_growth_m2 < 0.25) THEN 1
                 WHEN year = first_appearance AND population_growth_p1 < 0.33 THEN 1
                ELSE 0
            END AS anomaly_flag
    FROM census_place_stable_growth
)
SELECT * FROM census_place_stable_population_with_anomaly_flags