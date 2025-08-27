WITH year_range AS (
    SELECT DISTINCT year
    FROM {{ ref('usa_census_place_stable_population_with_anomaly_flags') }}
),
nhgis_census_place_stable_unique AS (
    SELECT DISTINCT census_place_id
    FROM {{ ref('usa_census_place_stable_population_with_anomaly_flags') }}
),
nhgis_census_place_stable_skeleton AS (   
    SELECT  census_place_id, 
            year
    FROM nhgis_census_place_stable_unique
    CROSS JOIN year_range
),
nhgis_census_place_stable_skeleton_filled AS (
    SELECT  census_place_id,
            year,
            anomaly_flag,
            population
    FROM nhgis_census_place_stable_skeleton
    LEFT JOIN {{ ref('usa_census_place_stable_population_with_anomaly_flags') }}
    USING (census_place_id, year)
),
nhgis_census_place_stable_population_interpolated AS (
    WITH nhgis_census_place_stable_anomaly_to_null AS (
        SELECT  census_place_id,
                year,
                CASE WHEN anomaly_flag = 1 THEN NULL ELSE population END AS population
        FROM nhgis_census_place_stable_skeleton_filled
    ),
    nhgis_census_place_stable_lag_lead_non_null_values_auxiliary_partitions AS (
        SELECT  census_place_id,
                year,
                population,
                SUM(CASE WHEN population IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY census_place_id ORDER BY year) AS partition_lag_non_null,
                SUM(CASE WHEN population IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY census_place_id ORDER BY year DESC) AS partition_lead_non_null
        FROM nhgis_census_place_stable_anomaly_to_null
    ),
    nhgis_census_place_stable_lag_lead_non_null_values AS (
        SELECT  census_place_id, 
                year, 
                population,
                FIRST_VALUE(population) OVER (PARTITION BY census_place_id, partition_lag_non_null ORDER BY year) AS value_lag_non_null,
                FIRST_VALUE(population) OVER (PARTITION BY census_place_id, partition_lead_non_null ORDER BY year DESC) AS value_lead_non_null,
                FIRST_VALUE(year) OVER (PARTITION BY census_place_id, partition_lag_non_null ORDER BY year) AS year_lag_non_null,
                FIRST_VALUE(year) OVER (PARTITION BY census_place_id, partition_lead_non_null ORDER BY year DESC) AS year_lead_non_null
        FROM nhgis_census_place_stable_lag_lead_non_null_values_auxiliary_partitions
    ),
    nhgis_census_place_stable_interpolation AS (
        SELECT  census_place_id,
                year,
                CASE WHEN population IS NOT NULL THEN population
                    WHEN value_lag_non_null IS NOT NULL AND value_lead_non_null IS NOT NULL THEN value_lag_non_null + (year - year_lag_non_null) * (value_lead_non_null - value_lag_non_null) / (year_lead_non_null - year_lag_non_null)
                    ELSE NULL
                END AS population,
                CASE WHEN population IS NULL THEN 1
                    ELSE 0
                END AS population_interpolated
        FROM nhgis_census_place_stable_lag_lead_non_null_values
    )
    SELECT  census_place_id, 
            year, 
            population,
            population_interpolated
    FROM nhgis_census_place_stable_interpolation
    WHERE population IS NOT NULL
)
SELECT * 
FROM nhgis_census_place_stable_population_interpolated