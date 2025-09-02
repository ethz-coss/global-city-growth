SELECT  g.cluster_id,
        g.y1,
        g.y2,
        g.urban_threshold,
        COALESCE(y1s.pop, 0) AS population_y1,
        COALESCE(y2s.pop, 0) AS population_y2
FROM {{ ref('world_cluster_growth_geom') }} AS g
LEFT JOIN LATERAL (
  SELECT (ST_SummaryStatsAgg(ST_Clip(r.rast, 1, g.geom, true), 1, true)).sum AS pop
  FROM {{ source('ghsl', 'world_raster_ghsl_pop_all_years') }} AS r
  WHERE r.year = g.y1 AND ST_Intersects(r.rast, g.geom)
) AS y1s ON TRUE
LEFT JOIN LATERAL (
  SELECT (ST_SummaryStatsAgg(ST_Clip(r.rast, 1, g.geom, true), 1, true)).sum AS pop
  FROM {{ source('ghsl', 'world_raster_ghsl_pop_all_years') }} AS r
  WHERE r.year = g.y2 AND ST_Intersects(r.rast, g.geom)
) AS y2s ON TRUE
