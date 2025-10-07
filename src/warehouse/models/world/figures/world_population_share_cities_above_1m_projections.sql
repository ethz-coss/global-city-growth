-- The equation to predict the population share of cities above one million in 2060 is:
-- s = (x_max^{1-alpha} - z^{1-alpha}) / (x_max^{1-alpha} - x_min^{1-alpha})
-- Here x_max is the urban population in 2060, z is 1 million, x_min is 5 thousand, and alpha is the zipf exponent (1 / rank_size_slope)

-- We need to add these otherwise the macro will not work
-- depends_on: {{ ref('world_urban_population') }}
-- depends_on: {{ source('owid', 'world_country_region') }}
-- depends_on: {{ ref('world_population_share_cities_above_1m_historical') }}
{% set rank_size_slope_table = ref('world_rank_size_slopes') %}

{{ get_population_share_cities_above_1m_projections(rank_size_slope_table) }}