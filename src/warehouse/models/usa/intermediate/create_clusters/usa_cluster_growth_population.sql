WITH crosswalk_census_place_stable_to_cluster_growth AS (
    SELECT  cg.cluster_id, 
            cg.y1,
            cg.y2,
            cg.pixel_threshold,
            ncpg.census_place_id
    FROM {{ ref('usa_cluster_growth_geom') }} cg 
    INNER JOIN {{ ref('usa_census_place_stable_geom') }} ncpg
    ON ST_Intersects(cg.geom, ncpg.geom)
),
cluster_growth_pop_y1 AS (
    SELECT  cw.cluster_id, 
            cw.y1,
            cw.y2,
            cw.pixel_threshold,
            SUM(ncp.population) AS population_y1
    FROM crosswalk_census_place_stable_to_cluster_growth cw
    INNER JOIN {{ ref('usa_census_place_stable_population')}} ncp
    ON cw.census_place_id = ncp.census_place_id
    AND cw.y1 = ncp.year
    GROUP BY cw.cluster_id, cw.y1, cw.y2, cw.pixel_threshold
),
cluster_growth_pop_y2 AS (
    SELECT  cw.cluster_id, 
            cw.y1,
            cw.y2,
            cw.pixel_threshold,
            SUM(ncp.population) AS population_y2
    FROM crosswalk_census_place_stable_to_cluster_growth cw
    INNER JOIN {{ ref('usa_census_place_stable_population')}} ncp
    ON cw.census_place_id = ncp.census_place_id
    AND cw.y2 = ncp.year
    GROUP BY cw.cluster_id, cw.y1, cw.y2, cw.pixel_threshold
),
cluster_growth_pop AS (
    SELECT  cluster_id,
            y1,
            y2,
            pixel_threshold,
            cgpy1.population_y1,
            cgpy2.population_y2
    FROM cluster_growth_pop_y1 cgpy1
    INNER JOIN cluster_growth_pop_y2 cgpy2
    USING (cluster_id, y1, y2, pixel_threshold)
)
SELECT * FROM cluster_growth_pop
ORDER BY cluster_id, pixel_threshold, y1, y2