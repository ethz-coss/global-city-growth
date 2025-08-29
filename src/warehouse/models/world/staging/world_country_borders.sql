SELECT  cntry_name, 
        gwcode, 
        gwsyear, 
        gwsmonth, 
        gwsday, 
        gweyear, 
        gwemonth, 
        gweday, 
        ST_SetSrid(geometry, 4326) AS geom
FROM {{ source('cshapes', 'world_country_borders_raw') }}