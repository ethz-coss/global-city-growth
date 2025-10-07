WITH city_population AS (
    SELECT  cluster_id,
            y1 AS year,
            country, 
            analysis_id,
            population_y1,
            population_y2,
            ROW_NUMBER() OVER (PARTITION BY y1, country ORDER BY population_y1 DESC) AS city_rank,
            CASE WHEN population_y1 >= 5 * POWER(10, 6) THEN 1 ELSE 0 END AS above_5M,
            CASE WHEN population_y1 >= POWER(10, 6) THEN 1 ELSE 0 END AS above_1M,
            CASE WHEN population_y1 < POWER(10, 6) THEN 1 ELSE 0 END AS below_1M
    FROM {{ ref('world_cluster_growth_population_country_analysis') }}
    WHERE y1 + 10 = y2
),
average_growth_above_1M AS (
    SELECT  country, 
            year, 
            analysis_id,
            'above_1M' AS city_group,
            LOG(SUM(population_y2) / SUM(population_y1)) AS log_average_growth
    FROM city_population
    WHERE above_1M = 1
    GROUP BY country, year, analysis_id
),
average_growth_below_1M AS (
    SELECT  country, 
            year, 
            analysis_id,
            'below_1M' AS city_group,
            LOG(SUM(population_y2) / SUM(population_y1)) AS log_average_growth
    FROM city_population
    WHERE below_1M = 1
    GROUP BY country, year, analysis_id
),
average_growth_group AS (
    SELECT *
    FROM average_growth_above_1M
    UNION ALL
    SELECT *
    FROM average_growth_below_1M
),
num_cities AS (
    SELECT  country, 
            year, 
            analysis_id,
            COUNT(DISTINCT cluster_id) AS num_cities
    FROM city_population
    GROUP BY country, year, analysis_id
),
average_growth_group_demeaned AS (
    SELECT  a.country, 
            a.year, 
            a.analysis_id,
            g.city_group,
            g.log_average_growth AS log_average_growth_group,
            g.log_average_growth - a.log_average_growth AS log_average_growth_group_demeaned,
            a.region,
            n.num_cities
    FROM average_growth_group g
    INNER JOIN {{ ref('world_average_growth') }} a
    USING (country, year, analysis_id)
    INNER JOIN num_cities n
    USING (country, year, analysis_id)
)
SELECT * 
FROM average_growth_group_demeaned