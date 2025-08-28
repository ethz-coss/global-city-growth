{{ config(
    materialized = 'incremental',
    strategy = 'delete+insert',
    pre_hook = "delete from {{ this }} where my_var = {{ var('my_var') | int }}"
)
}}

{% set my_var_value = var('my_var') | int %}


SELECT {{ my_var_value }} * a AS a, b, c, {{ my_var_value }} AS my_var, now() AS last_updated
FROM {{ source('convolved_raster', 'my_example_asset') }}