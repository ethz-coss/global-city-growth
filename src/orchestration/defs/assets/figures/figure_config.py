# src/orchestration/defs/assets/figures/figure_config.py
import plotly.express as px

style_config = {
    'font_name': 'Helvetica',
    'font_family': 'Helvetica',
    'title_font_size': 14,
    'axis_font_size': 12,
    'inset_font_size': 10,
    'tick_font_size': 10,
    'inset_tick_font_size': 8,
    'inset_text_label_font_size': 8,
    'colorbar_label_font_size': 12,
}

region_colors = {
    'Africa': px.colors.qualitative.Plotly[1],
    'Americas': px.colors.qualitative.Plotly[2],
    'Asia': px.colors.qualitative.Plotly[3],
    'Europe': px.colors.qualitative.Plotly[5]
}

figure_dir = '/app/figures'
figure_si_dir = '/app/figures/si'
table_dir = '/app/figures/tables'

MAIN_ANALYSIS_ID = 1