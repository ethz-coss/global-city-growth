{% set my_var_value = var('my_var') | int %}

{{ config(
    materialized = 'incremental',
    strategy = 'delete+insert',
    unique_key = ['id', 'my_var'],
    incremental_predicates = ["my_var = " ~ my_var_value]
)
}}


SELECT id, {{ my_var_value }} * a AS a, b, c, {{ my_var_value }} AS my_var, now() AS last_updated
FROM {{ source('incremental_example', 'my_example_asset') }}