{% set hist_years = var('constants')['IPUMS_HISTORICAL_YEARS'] %}
{% set modern_years = var('constants')['NHGIS_POP_YEARS'] %}

WITH 
{% for years in [hist_years, modern_years] %}
    {% for y1 in years %}
        {% for y2 in years %}
            {% if y1 <= y2 %}
                left_{{ y1 }}_{{ y2 }} AS (
                    SELECT  cluster_id, 
                            pixel_threshold,
                            geom
                    FROM {{ ref('usa_cluster_base_geom') }}
                    WHERE year = {{ y1 }}
                ),
                right_{{ y1 }}_{{ y2 }} AS (
                    SELECT  cluster_id, 
                            pixel_threshold,
                            geom
                    FROM {{ ref('usa_cluster_base_geom') }}
                    WHERE year = {{ y2 }}
                ),
                intersection_matching_{{ y1 }}_{{ y2 }} AS (
                    SELECT  l.cluster_id AS left_cluster_id, 
                            r.cluster_id AS right_cluster_id,
                            l.pixel_threshold AS pixel_threshold
                    FROM left_{{ y1 }}_{{ y2 }}  l 
                    LEFT JOIN right_{{ y1 }}_{{ y2 }} r
                    ON ST_Intersects(r.geom, l.geom)
                    AND l.pixel_threshold = r.pixel_threshold
                ),
            {% endif %}
        {% endfor %}
    {% endfor %}
{% endfor %}
cluster_matching_union AS (
{% set queries = [] %}
{% for years in [hist_years, modern_years] %}
    {% for y1 in years %}
        {% for y2 in years %}
            {% if y1 <= y2 %}
                    {% set query = "SELECT " ~ y1 ~ " AS y1, " ~ y2 ~ " AS y2, left_cluster_id, right_cluster_id, pixel_threshold FROM intersection_matching_" ~ y1 ~ "_" ~ y2 %}
                    {% do queries.append(query) %}
                
            {% endif %}
        {% endfor %}
    {% endfor %}
{% endfor %}
{{ queries | join(' UNION ALL\n') }}
)
SELECT * FROM cluster_matching_union