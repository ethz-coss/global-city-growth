WITH size_vs_growth_above_1m AS (
    SELECT  country, 
            year, 
            analysis_id, 
            log_population, 
            normalized_log_growth,
            region,
            'above_1m' AS city_group
    FROM {{ ref('world_size_vs_growth_normalized') }}
    WHERE log_population >= 6
),
size_vs_growth_below_1m AS (
    SELECT  country, 
            year, 
            analysis_id, 
            log_population, 
            normalized_log_growth,
            region,
            'below_1m' AS city_group
    FROM {{ ref('world_size_vs_growth_normalized') }}
    WHERE log_population < 6
),
size_vs_growth_largest_city AS (
    WITH ranked_cities AS (
        SELECT  *,
                ROW_NUMBER() OVER (PARTITION BY country, year, analysis_id ORDER BY log_population DESC) AS city_rank
        FROM {{ ref('world_size_vs_growth_normalized') }}
    )
    SELECT  country, 
            year, 
            analysis_id, 
            log_population, 
            normalized_log_growth,
            region,
            'largest_city' AS city_group
    FROM ranked_cities
    WHERE city_rank = 1
),
size_vs_growth_by_group AS (
    SELECT *
    FROM size_vs_growth_above_1m
    UNION ALL
    SELECT *
    FROM size_vs_growth_below_1m
    UNION ALL
    SELECT *
    FROM size_vs_growth_largest_city
),
size_vs_growth_by_group_exponentiated AS (
    SELECT  country, 
            year, 
            analysis_id, 
            city_group, 
            region,
            POWER(10, log_population) AS population,
            POWER(10, normalized_log_growth) - 1 AS normalized_growth
    FROM size_vs_growth_by_group
)
SELECT *
FROM size_vs_growth_by_group_exponentiated