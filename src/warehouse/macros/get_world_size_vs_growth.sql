{% macro get_world_size_vs_growth(urban_threshold, city_population_threshold, country_min_num_cities, country_exclude, analysis_id) %}
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
    size_vs_growth_cities AS (
        SELECT  cluster_id,
                y1 AS year,
                country,
                LOG(population_y1) AS log_population,
                LOG(population_y2 / population_y1) AS log_growth
        FROM {{ ref('world_cluster_growth_population_country') }}
        WHERE y2 = y1 + 10 AND population_y1 > {{ city_population_threshold }} AND urban_threshold = {{ urban_threshold }}
    )
    SELECT cluster_id,
           year,
           country,
           log_population,
           log_growth,
           {{ analysis_id }} AS analysis_id
    FROM size_vs_growth_cities
    WHERE country IN (SELECT country FROM countries)
{% endmacro %}