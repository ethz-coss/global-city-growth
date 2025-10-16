-- See src/warehouse/macros/get_population_share_cities_above_1m_projections.sql for explanation

-- depends_on: {{ ref('world_urban_population') }}
-- depends_on: {{ source('owid', 'world_country_region') }}
{% set rank_size_slope_table = source('figure_data_prep', 'world_rank_size_slopes_historical_ols') %}

{{ get_population_share_cities_above_1m_projections(rank_size_slope_table) }}