WITH growth_rates_grouped AS (
    SELECT  country, 
            year, 
            log_size,
            log_growth,
            analysis_id
    FROM {{ ref('world_size_vs_growth') }}