{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
)}}