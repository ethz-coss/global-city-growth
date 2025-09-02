WITH cluster_growth_pop_with_world_bank_code AS (
    SELECT  cl.cluster_id, 
            cl.y1, 
            cl.y2, 
            cl.urban_threshold,
            cl.population_y1, 
            cl.population_y2, 
            cw.world_bank_code AS country
    FROM {{ ref('world_cluster_growth_population') }} cl
    INNER JOIN {{ ref('world_cluster_growth_geocoding') }} gc
    USING (cluster_id, y1, y2, urban_threshold)
    INNER JOIN {{ source('cshapes', 'world_crosswalk_cshapes_code_to_iso_code') }} cw
    ON gc.gwcode = cw.cshapes_code
)
SELECT * 
FROM cluster_growth_pop_with_world_bank_code
WHERE country IS NOT NULL
ORDER BY country, urban_threshold, y1, y2,cluster_id