{% set years = var('constants')['USA_YEARS'] %}

WITH usa_raster AS (
    SELECT rast 
    FROM {{ ref('usa_template_raster') }}
),
{% for year in years %}
    census_place_pop_geom_{{year}} AS (
        SELECT  cpp.census_place_id, 
                cpp.population, 
                cpg.geom
        FROM {{ ref('usa_census_place_stable_population') }} cpp
        INNER JOIN {{ ref('usa_census_place_stable_geom') }} cpg
        USING (census_place_id)
        WHERE cpp.year = {{ year }}
    ),
    census_places_geomval_{{year}} AS (
        SELECT ARRAY_AGG((geom::geometry, population::float)::geomval) AS geomvalset
        FROM census_place_pop_geom_{{year}}
    ),
    census_place_rast_{{year}} AS (
        SELECT ST_SetValues(usa_raster.rast, 1, census_places_geomval_{{year}}.geomvalset, FALSE) AS rast
        FROM census_places_geomval_{{year}}
        CROSS JOIN usa_raster
    ),
{% endfor %}
census_place_raster AS (
    {% for year in years %}

        SELECT  rast, 
                {{ year }} AS year
        FROM census_place_rast_{{year}}

    {% if not loop.last %}
    UNION ALL
    {% endif %}

    {% endfor %}
)
SELECT * FROM census_place_raster