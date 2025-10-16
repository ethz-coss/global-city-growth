-- We need to add these otherwise the macro will not work
-- depends_on: {{ source('world_analysis_python', 'world_rank_size_slopes_historical') }}
{% set size_growth_slope_projections_table = source('world_analysis_python', 'world_size_growth_slopes_projections') %}

{{ get_rank_size_slopes_projections(size_growth_slope_projections_table) }}
