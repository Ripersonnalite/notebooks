import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
load_figure_template("minty")

df = pd.read_csv("Crypto_Detailed.csv")
df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Split', 'Crypto']
df["Date"] = pd.to_datetime(df["Date"])
df["year"] = pd.DatetimeIndex(df["Date"]).year
app = dash.Dash(external_stylesheets=[dbc.themes.MINTY])

app.layout = html.Div(children=[dbc.Row([
    html.H5("Dashboards with Python Dash"),
    dcc.Graph(id='graph-with-slider'),
    dbc.Row([
        dbc.Col(
            dcc.Slider(
                id='year-slider1',
                min=df['year'].min(),
                max=df['year'].max(),
                value=df['year'].min(),
                marks={str(year): str(year) for year in df['year'].unique()},
                step=None
            ), sm=4),
        dbc.Col(dcc.Slider(
        id='year-slider2',
        min=df['year'].min(),
        max=df['year'].max(),
        value=df['year'].max(),
        marks={str(year): str(year) for year in df['year'].unique()},
        step=None
    ), sm=4),
    dbc.Col(dcc.Dropdown(
                    options=[{'label':x, 'value':x} for x in df["Crypto"].unique()] + [{'label': 'Select all', 'value': 'all_values'}], 
                    multi=True,
                    value='all_values',
                    id='dropdown'
                    )
                , sm=4)
        ])
    ])
])

#Decorator
@app.callback(
    Output('graph-with-slider', 'figure'),
    [Input('year-slider1', 'value'),
    Input('year-slider2', 'value'),
    Input('dropdown', 'value'),
    ])
def update_figure(selected_year1,selected_year2, crypto):
"""By Ricardo Kazuo"""
    if crypto == 'all_values':
        filtered_df = df[(df.year >= selected_year1)&(df.year <= selected_year2)]
    else:
        filtered_df = df[(df.Crypto.isin(crypto))&(df.year >= selected_year1)&(df.year <= selected_year2)]
    fig = px.scatter(filtered_df, x="Date" , y="Close", 
                     color="Crypto", hover_name="Crypto", size_max=55)
    fig.update_layout(transition_duration=500, showlegend=False)
    print(crypto)
    return fig

if __name__ == '__main__':
    app.run_server(
        port=8000,
        host='0.0.0.0'
    )