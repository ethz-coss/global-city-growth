-- We need to add these otherwise the macro will not work
-- depends_on: {{ ref('world_urban_population') }}
-- depends_on: {{ source('owid', 'world_country_region') }}
-- depends_on: {{ ref('world_urb_pop_share_cities_above_1m_historical') }}
{% set rank_size_slope_table = ref('world_rank_size_slopes') %}

{{ get_population_share_cities_above_1m_projections(rank_size_slope_table) }}