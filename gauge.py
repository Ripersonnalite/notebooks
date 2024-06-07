import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import dash_daq as daq
import psutil
from flask import Flask, jsonify, render_template_string
from dash_bootstrap_templates import ThemeSwitchAIO
import plotly.graph_objs as go
from dash.dependencies import Input, Output

template_theme2 = "darkly"
template_theme1 = "flatly"
url_theme2 = dbc.themes.DARKLY
url_theme1 = dbc.themes.FLATLY

server = Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP])

total_memory = psutil.virtual_memory().total / (1024**3)  # Convert bytes to GB
total_disk = psutil.disk_usage('/').total / (1024**3)  # Convert bytes to GB

# Function to create gauge chart
def create_gauge(title, value, max_value):
    gauge = go.Figure(go.Indicator(
        mode='gauge+number',
        value=value,
        title={'text': title},
        gauge={'axis': {'range': [0, max_value]}}
    ))
    gauge.update_layout(autosize=False, width=500, height=300)
    return gauge.to_json()

def get_system_stats():
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    cpu = psutil.cpu_percent(interval=1)
    return {
        "memory_percent": memory.percent,
        "disk_percent": disk.percent,
        "cpu_percent": cpu
    }




# Define the Flask route for memory usage gauge
@server.route('/cpu')
def cpu_gauge():
    cpu_usage = psutil.cpu_percent(interval=1)
    graph_json = create_gauge('CPU Usage', cpu_usage, 100)
    
    # HTML template string with embedded Plotly.js to render the gauge
    html_string = f'''
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>

    </head>
    <body>
        <button onclick="window.history.back();" class="back-button">Go Back</button>
        <div id="gauge" style="width: 500px; height: 300px;"></div>
        <script>
            var gaugeData = {graph_json};
            Plotly.newPlot('gauge', gaugeData.data, gaugeData.layout);
            
            // Function to fetch new memory usage data and update the gauge
            function updateGauge() {{
                fetch('/cpu-data')
                .then(response => response.json())
                .then(data => {{
                    Plotly.update('gauge', {{
                        'value': [data.percent]
                    }}, {{
                        'gauge': {{ 'axis': {{ 'range': [0, 100] }} }}
                    }});
                }});
            }}
            
            // Set an interval to update the gauge every 5 seconds
            setInterval(updateGauge, 5000);
        </script>
    </body>
    </html>
    '''
    return render_template_string(html_string)

# Define a new Flask route to provide memory usage data in JSON format
@server.route('/cpu-data')
def cpu_data():
    cpu_usage = psutil.cpu_percent(interval=1)
    return {'percent': cpu_usage}







# Define the Flask route for memory usage gauge
@server.route('/memory')
def memory_gauge():
    memory_usage = psutil.virtual_memory().percent
    graph_json = create_gauge('Memory Usage', memory_usage, 100)
    
    # HTML template string with embedded Plotly.js to render the gauge
    html_string = f'''
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>

    </head>
    <body>
        <button onclick="window.history.back();" class="back-button">Go Back</button>
        <div id="gauge" style="width: 500px; height: 300px;"></div>
        <script>
            var gaugeData = {graph_json};
            Plotly.newPlot('gauge', gaugeData.data, gaugeData.layout);
            
            // Function to fetch new memory usage data and update the gauge
            function updateGauge() {{
                fetch('/memory-data')
                .then(response => response.json())
                .then(data => {{
                    Plotly.update('gauge', {{
                        'value': [data.percent]
                    }}, {{
                        'gauge': {{ 'axis': {{ 'range': [0, 100] }} }}
                    }});
                }});
            }}
            
            // Set an interval to update the gauge every 5 seconds
            setInterval(updateGauge, 5000);
        </script>
    </body>
    </html>
    '''
    return render_template_string(html_string)

# Define a new Flask route to provide memory usage data in JSON format
@server.route('/memory-data')
def memory_data():
    memory_usage = psutil.virtual_memory().percent
    return {'percent': memory_usage}

# Define the Flask route for disk usage gauge
@server.route('/disk')
def disk_gauge():
    disk_usage = psutil.disk_usage('/').percent
    graph_json = create_gauge('Disk Usage', disk_usage, 100)
    
    # HTML template string with embedded Plotly.js to render the gauge
    html_string = f'''
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>

    </head>
    <body>
        <button onclick="window.history.back();" class="back-button">Go Back</button>
        <div id="gauge" style="width: 500px; height: 300px;"></div>
        <script>
            var gaugeData = {graph_json};
            Plotly.newPlot('gauge', gaugeData.data, gaugeData.layout);
            
            // Function to fetch new disk usage data and update the gauge
            function updateGauge() {{
                fetch('/disk-data')
                .then(response => response.json())
                .then(data => {{
                    Plotly.update('gauge', {{
                        'value': [data.percent]
                    }}, {{
                        'gauge': {{ 'axis': {{ 'range': [0, 100] }} }}
                    }});
                }});
            }}
            
            // Set an interval to update the gauge every 5 seconds
            setInterval(updateGauge, 5000);
        </script>
    </body>
    </html>
    '''
    return render_template_string(html_string)

# Callback to update cpu gauge
@app.callback(
    Output('cpu-gauge', 'value'),
    [Input('interval-component', 'n_intervals')]
)
def update_memory_gauge(n):
    return get_system_stats()['cpu_percent']
    
# Callback to update memory gauge
@app.callback(
    Output('memory-gauge', 'value'),
    [Input('interval-component', 'n_intervals')]
)
def update_memory_gauge(n):
    return get_system_stats()['memory_percent']

# Callback to update disk gauge
@app.callback(
    Output('disk-gauge', 'value'),
    [Input('interval-component', 'n_intervals')]
)
def update_disk_gauge(n):
    return get_system_stats()['disk_percent']

# Define a new Flask route to provide disk usage data in JSON format
@server.route('/disk-data')
def disk_data():
    disk_usage = psutil.disk_usage('/').percent
    return {'percent': disk_usage}  

memory_card = dbc.Card(
    dbc.CardBody([
        html.A(
        daq.Gauge(
            id='memory-gauge',
            label=f'Memory Usage (Total: {total_memory:.2f} GB)',
            value=get_system_stats()['memory_percent'],
            min=0,
            max=100,
            size=100,
            showCurrentValue=True,
            units="%",
            color={"gradient":True,"ranges":{"green":[0,60],"yellow":[60,80],"red":[80,100]}}
        ),
            href="/memory",  # Replace with your desired URL
        )
    ]),
    style={"width": "15rem", "height": "12rem", "margin": "5px"}
)

disk_card = dbc.Card(
    dbc.CardBody([
        html.A(
        daq.Gauge(
            id='disk-gauge',
            label=f'Disk Space (Total: {total_disk:.2f} GB)',
            value=get_system_stats()['disk_percent'],
            min=0,
            max=100,
            size=100,
            showCurrentValue=True,
            units="%",
            color={"gradient":True,"ranges":{"green":[0,60],"yellow":[60,80],"red":[80,100]}}
        ),
            href="/disk",  # Replace with your desired URL
        )
    ]),
    style={"width": "15rem", "height": "12rem", "margin": "5px"}
)

cpu_card = dbc.Card(
    dbc.CardBody([
        html.A(
        daq.Gauge(
            id='cpu-gauge',
            label="AMD Ryzen 7 5700U",
            value=get_system_stats()['cpu_percent'],
            min=0,
            max=100,
            size=100,
            showCurrentValue=True,
            units="%",
            color={"gradient":True,"ranges":{"green":[0,60],"yellow":[60,80],"red":[80,100]}}
        ),
            href="/cpu",  # Replace with your desired URL
        )
    ]),
    style={"width": "15rem", "height": "12rem", "margin": "5px"}
)

outer_card = dbc.Card(
    dbc.CardBody([
        dbc.Row([
            dbc.Col(ThemeSwitchAIO(aio_id="theme", themes=[url_theme1, url_theme2]), sm=4)
        ]),
        html.H3("System Resources Dashboard", className="card-title", style={'textAlign': 'center'}),
        dbc.Row([
            dbc.Col(memory_card, width="auto"),
            dbc.Col(disk_card, width="auto"),
            dbc.Col(cpu_card, width="auto")
        ], justify="around")
    ]),
    style={"margin": "20px"}
)

app.layout = html.Div([
    outer_card,
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # in milliseconds
        n_intervals=0
    )
])

if __name__ == '__main__':
    app.run_server(debug=False, port=8056)
