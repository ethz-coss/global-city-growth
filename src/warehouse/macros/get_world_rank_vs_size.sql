{% macro get_world_rank_vs_size(urban_threshold, city_population_threshold, country_min_num_cities, country_exclude, analysis_id) %}
    WITH countries AS (
        WITH num_cities_by_country_year AS (
            SELECT country, y1 AS year, COUNT(DISTINCT cluster_id) AS num_cities
            FROM {{ ref('world_cluster_growth_population_country') }}
            WHERE y1 = y2 AND population_y1 > {{ city_population_threshold }}
            GROUP BY country, year
        ),
        num_cities_by_country AS (
            SELECT country, MIN(num_cities) AS num_cities
            FROM num_cities_by_country_year
            GROUP BY country
        )
        SELECT country
        FROM num_cities_by_country
        WHERE num_cities >= {{ country_min_num_cities }}
        AND country NOT IN ({{ country_exclude }})
    ),
    rank_vs_size_cities AS (
        SELECT  cluster_id,
        y1 AS year,
        country,
        LOG(population_y1) AS log_population,
        LOG(ROW_NUMBER() OVER (PARTITION BY y1, country ORDER BY population_y1 DESC)) AS log_rank
        FROM {{ ref('world_cluster_growth_population_country') }}
        WHERE y1 = y2 AND population_y1 > {{ city_population_threshold }} AND urban_threshold = {{ urban_threshold }}
    )
    SELECT cluster_id,
           year,
           country,
           log_population,
           log_rank,
           {{ analysis_id }} AS analysis_id
    FROM rank_vs_size_cities
    WHERE country IN (SELECT country FROM countries)
{% endmacro %}