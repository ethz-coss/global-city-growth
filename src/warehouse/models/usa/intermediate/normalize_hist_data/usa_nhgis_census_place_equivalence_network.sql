--- The equivalence network links nhgis census place that we define as "equivalent". The edges of this network are decided based on the following rules:
--- 1. If a census place disappears from the data, we select a "continuation" place and link it to the census place that disappeared
--- This continuation place is: (a) the census place that has the highest migration probability to the disappearing place in the year of disappearance; (b) the census place that is the nearest neighbor of the disappearing place
--- 2. If two census places have a high migration probability, we link them.
--- The migration probability is considered high if it is higher than 0.3 and the distance between the places is less than 20000, or if it is higher than 0.5
--- 3. We link census place to themselves (because a place is always equivalent to itself)

WITH disappearing_places AS (
    SELECT census_place_id AS disappearing_place_id, last_appearance
    FROM {{ ref('usa_nhgis_census_place_appearances') }}
    WHERE last_appearance < 1940
),
disappearing_places_migration_continuation_candidates AS (
    WITH candidates AS (
        SELECT  dp.disappearing_place_id, 
                dp.last_appearance,
                mp.census_place_destination AS continuation_candidate_id,
                mp.migration_probability,
                nn.distance
        FROM disappearing_places dp
        INNER JOIN {{ ref('usa_nhgis_census_place_migration_probability') }} mp
        ON dp.disappearing_place_id = mp.census_place_origin AND dp.last_appearance = mp.year_origin
        LEFT JOIN {{ ref('usa_nhgis_census_place_nearest_neighbors') }} nn 
        ON dp.disappearing_place_id = nn.census_place_id AND mp.census_place_destination = nn.neighbor_census_place_id
    ),
    ranked_candidates AS (
        SELECT  disappearing_place_id, 
                continuation_candidate_id, 
                migration_probability,
                distance,
                RANK() OVER (PARTITION BY disappearing_place_id ORDER BY migration_probability DESC, distance ASC) AS rank
        FROM candidates
        WHERE distance < 50000
    )
    SELECT  disappearing_place_id, 
            continuation_candidate_id
    FROM ranked_candidates
    WHERE rank = 1
),
disappearing_places_distance_continuation_candidates AS (
    WITH candidates AS (
        SELECT  disappearing_place_id,
                nn.neighbor_census_place_id AS continuation_candidate_id,
                nn.distance
        FROM disappearing_places
        INNER JOIN {{ ref('usa_nhgis_census_place_nearest_neighbors') }} nn
        ON disappearing_place_id = nn.census_place_id
        WHERE disappearing_place_id != nn.neighbor_census_place_id
    ),
    ranked_candidates AS (
        SELECT  disappearing_place_id,
                continuation_candidate_id,
                distance,
                ROW_NUMBER() OVER (PARTITION BY disappearing_place_id ORDER BY distance ASC) AS rank
        FROM candidates
    )
    SELECT  disappearing_place_id, 
            continuation_candidate_id
    FROM ranked_candidates
    WHERE rank = 1
),
edges_disappearing_places AS (
    SELECT  disappearing_place_id AS left_census_place_id,
            COALESCE(mcc.continuation_candidate_id, dcc.continuation_candidate_id) AS right_census_place_id
    FROM disappearing_places
    LEFT JOIN disappearing_places_migration_continuation_candidates mcc
    USING (disappearing_place_id)
    LEFT JOIN disappearing_places_distance_continuation_candidates dcc
    USING (disappearing_place_id)
),
migration_and_distance AS (
    SELECT  mp.census_place_origin, 
            mp.census_place_destination, 
            mp.year_origin, 
            mp.year_destination, 
            mp.migration_probability, 
            COALESCE(nn.distance, 1000000000) as distance
    FROM {{ ref('usa_nhgis_census_place_migration_probability') }} mp
    LEFT JOIN {{ ref('usa_nhgis_census_place_nearest_neighbors') }} nn
    ON mp.census_place_origin = nn.census_place_id AND mp.census_place_destination = nn.neighbor_census_place_id
),
edges_migration_and_distance AS (
    SELECT  census_place_origin AS left_census_place_id, 
            census_place_destination AS right_census_place_id
    FROM (
        SELECT  census_place_origin, 
                census_place_destination, 
                CASE WHEN migration_probability > 0.3 AND distance < 50000 THEN 1
                     WHEN migration_probability > 0.5 THEN 1
                ELSE 0
                END as edge
        FROM migration_and_distance
    )
    WHERE edge = 1
),
identity_edges AS (
    SELECT census_place_id AS left_census_place_id, census_place_id AS right_census_place_id
    FROM {{ ref('usa_nhgis_census_place_geom') }}
),
edges AS (
    SELECT left_census_place_id, right_census_place_id
    FROM edges_disappearing_places
    UNION ALL
    SELECT left_census_place_id, right_census_place_id
    FROM edges_migration_and_distance
    UNION ALL
    SELECT left_census_place_id, right_census_place_id
    FROM identity_edges
),
edges_no_duplicates AS (
    SELECT DISTINCT left_census_place_id, right_census_place_id
    FROM (
        SELECT  LEAST(left_census_place_id, right_census_place_id) AS left_census_place_id,
                GREATEST(left_census_place_id, right_census_place_id) AS right_census_place_id
        FROM edges
    )
)
SELECT * FROM edges_no_duplicates