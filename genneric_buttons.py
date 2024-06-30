from flask import Flask
import requests
import dash
from dash import html, dcc
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
from dash_bootstrap_templates import ThemeSwitchAIO

template_theme2 = "flatly"
template_theme1 = "darkly"
url_theme2 = dbc.themes.FLATLY
url_theme1 = dbc.themes.DARKLY

ga_tracking_id = ""

var_style = {'width': '130px', 'margin': '5px', 'height': '45px', 'vertical-align': 'middle'}

var_outter_cardstyle = {
    'backgroundColor': 'rgba(0, 0, 0, 0.0)',
    'textAlign': 'center',
    'height': '600px',  # Height adjusts to content
}
var_cardstyle = {
    'backgroundColor': 'rgba(0, 0, 0, 0.3)',
    'textAlign': 'center',
    'height': '600px',
    'width': '170px',
    'border': 'none'
}
var_cardstyle_content = {
    'backgroundColor': 'rgba(0, 0, 0, 0.3)',
    'textAlign': 'center',
    'height': '600px',
    'width': '830px',
    'border': 'none'
}

try:
    # Read the title from a file with structured content
    with open('../Translations/structure.txt', 'r') as file:
        line = file.readline().strip()
        # Assuming the structure is always 'Button:home:"Title"'
        button_title = line.split(':')[2].strip('"')
except: button_title="Home"

# Initialize the Flask app
server = Flask(__name__)

# Initialize Dash by passing the Flask server
app = dash.Dash(__name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.index_string = f'''
<!DOCTYPE html>
<html>
    <head>
        {{%metas%}}
        <title>{{%title%}}</title>
        {{%favicon%}}
        {{%css%}}
        <!-- Global site tag (gtag.js) - Google Analytics -->
        <script async src="https://www.googletagmanager.com/gtag/js?id={ga_tracking_id}"></script>
        <script>
            window.dataLayer = window.dataLayer || [];
            function gtag(){{ dataLayer.push(arguments); }}
            gtag('js', new Date());
            gtag('config', '{ga_tracking_id}');
        </script>
    </head>
    <body>
        {{%app_entry%}}
        <footer>
            {{%config%}}
            {{%scripts%}}
            {{%renderer%}}
        </footer>
    </body>
</html>
'''

# Get button names from the file
file_path = 'buttons.txt'
buttons = []
button_ids = []
with open(file_path, 'r') as file:
    button_lines = [line.strip() for line in file.readlines()]

button_urls = {}
for line in button_lines:
    name, url = line.split(',')
    button_id = name.lower().replace(" ", "_")
    button_text = name.replace("btn-", "").title()
    button = html.Button(button_text, id=button_id, n_clicks=0, style=var_style)
    buttons.append(button)
    button_ids.append(button_id)
    button_urls[button_id] = url  # Add the URL to the dictionary



main_layout = html.Div(
    [
        dbc.Card(dbc.CardBody(
    [
        dbc.CardHeader(
            dbc.Row([
                dbc.Col(dbc.Button(button_title, href="https://main.ricardo.expert/", color="primary", style={'padding-top': '0px', 'padding-bottom': '0px'}), width=2),
                dbc.Col(html.Div("The Science", style={'textAlign': 'center', 'fontSize': '24px'}), width=8),
                dbc.Col(ThemeSwitchAIO(aio_id="theme", themes=[url_theme1, url_theme2]), width=2)
            ], className="g-0", justify="between"),
            style={'textAlign': 'center'}
        ),
        dbc.CardBody([
            dcc.Location(id='url', refresh=False),
            dbc.Row([
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody([
                            html.Div(buttons)
                        ]), style=var_cardstyle
                    ), style=var_outter_cardstyle
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody([
                            html.Div(id='page-content')
                        ]), style=var_cardstyle_content
                    ),
                    width='auto'
                )
            ])
        ]),
    ]
),
    style={
        'background-color': 'rgba(0, 0, 0, 0.5)',
        'margin': '20px',
        'background-image': 'url("./assets/diagram.jpeg")',  # Replace with your actual image file name
        'background-size': 'cover',  # This will ensure that the background covers the entire card area
        'background-repeat': 'no-repeat',  # This will prevent the image from repeating
        'background-position': 'center center'  # This will position the image at the center of the card
        
    })],
    style={
        'display': 'flex',  # Use Flexbox for the container
        'justifyContent': 'center',  # Center horizontally
        'height': '100vh',  # Take up full viewport height
        'background-color': 'rgba(0, 0, 0, 0.5)',
    }
)


app.layout = main_layout

# Create callback
@app.callback(
    Output('page-content', 'children'),
    [Input(button_id, 'n_clicks') for button_id in [name for name in button_ids]],
    [State('url', 'pathname')]
)
def display_report(*args):
    ctx = dash.callback_context
    if not ctx.triggered:
        # If no buttons were clicked, raise PreventUpdate to avoid updating the output
        raise PreventUpdate

    # Get the ID of the button that triggered the callback
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    # Get the URL associated with the button from the dictionary
    url = button_urls.get(button_id)

    if url is not None:
        return html.Iframe(
            src=url,
            style={"height": "500px", "width": "800px"}
        )

# Run the app on port 8080
if __name__ == '__main__':
    app.run_server(debug=True, port=8059)
