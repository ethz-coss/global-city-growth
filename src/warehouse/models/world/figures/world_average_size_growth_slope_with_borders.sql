WITH country_borders AS (
    SELECT world_bank_code AS country, geom
    FROM {{ ref('world_country_borders_2019') }} b
    INNER JOIN {{ source('cshapes', 'world_crosswalk_cshapes_code_to_iso_code') }} cw
    ON b.gwcode = cw.cshapes_code
),
average_size_growth_slope AS (
    SELECT  country, 
            analysis_id,
            AVG(size_growth_slope) AS size_growth_slope
    FROM {{ source('figure_data_prep', 'world_size_growth_slopes') }}
    GROUP BY country, analysis_id
),
average_size_growth_slope_with_borders AS (
    SELECT country, size_growth_slope, geom, analysis_id
    FROM country_borders
    LEFT JOIN average_size_growth_slope
    USING (country)
)
SELECT * FROM average_size_growth_slope_with_borders