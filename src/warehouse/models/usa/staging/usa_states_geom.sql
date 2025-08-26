SELECT  "State_Code" AS state_code, 
        "State_Name" AS state_name, 
        ST_Transform(geometry, 5070) AS geom
FROM {{ source('misc', 'usa_states_geom_raw') }} 