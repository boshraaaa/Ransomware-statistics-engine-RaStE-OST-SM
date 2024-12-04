from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
from influxdb_client import InfluxDBClient

# Create Dash App
app = Dash(__name__)
app.title = "Ransomware Dashboard"


# Connection details
url = "http://localhost:8086"  # Replace with your InfluxDB URL
token = "mTe9pXgfncRUZco9AG3k5tXG1ruePoRhJq-LF2B29yJ6bHTaHMT7l3VkmtUYS9raJbDhTq4kOnTt65YvkAHygA=="  # Replace with your token
org = "ransomeware"      # Replace with your organization
bucket = "ransomware"         # Replace with your bucket


try:
    # Initialize the client
    client = InfluxDBClient(url=url, token=token, org=org)

    # Test the connection by fetching available buckets
    buckets = client.buckets_api().find_buckets()
    print("Connection successful! Buckets available:")
    for bucket in buckets.buckets:
        print(f"- {bucket.name}")

except Exception as e:
    print(f"Failed to connect to InfluxDB: {e}")
finally:
    client.close()

# Function to fetch data from InfluxDB and return as a DataFrame
def fetch_influxdb_data(query):
    # Connect to InfluxDB
    client = InfluxDBClient(url=url, token=token, org=org)
    query_api = client.query_api()

    # Run the query and return as a DataFrame
    result = query_api.query_data_frame(query)
    client.close()

    return result

# Function to return query data (similar format to mock data)
def get_query_data(query_name):
    if query_name == "top_10_targets_query":
        # Define InfluxDB query to get top 10 targets (replace with your actual query)
        query = '''
        from(bucket: "ransomware")
            |> range(start: -1y)  // Adjust the time range as needed
            |> filter(fn: (r) => r._measurement == "attacks")
            |> filter(fn: (r) => r._field == "target_country")
            |> group(columns: ["target_country"])
            |> count()
            |> top(n: 10, columns: ["_value"])
        '''
        # Fetch the data from InfluxDB and format it
        influx_data = fetch_influxdb_data(query)
        # Rename the columns to match the mock data format
        influx_data = influx_data.rename(columns={"target_country": "Country", "_value": "Count"})
        # Return the first 10 rows
        return influx_data.head(10)

    elif query_name == "top_10_sources_query":
        # Define InfluxDB query to get top 10 sources (replace with your actual query)
        query = '''
        from(bucket: "ransomware")
            |> range(start: -1y)  // Adjust the time range as needed
            |> filter(fn: (r) => r._measurement == "attacks")
            |> filter(fn: (r) => r._field == "source_country")
            |> group(columns: ["source_country"])
            |> count()
            |> top(n: 10, columns: ["_value"])
        '''
        # Fetch the data from InfluxDB and format it
        influx_data = fetch_influxdb_data(query)
        # Rename the columns to match the mock data format
        influx_data = influx_data.rename(columns={"source_country": "Country", "_value": "Count"})
        # Return the first 10 rows
        return influx_data.head(10)

    # Add similar queries for other dashboard visualizations
    return pd.DataFrame()

'''
# Mock data for now (replace these with results from your queries)
def get_query_data(query_name):
    if query_name == "top_10_targets_query":
        return pd.DataFrame({
            "Country": ["US", "UK", "IN", "CN", "FR", "RU", "DE", "JP", "AU", "CA"],
            "Count": [150, 140, 130, 120, 110, 100, 90, 80, 70, 60]
        })
    elif query_name == "top_10_sources_query":
        return pd.DataFrame({
            "Country": ["RU", "CN", "US", "IR", "IN", "SY", "BR", "DE", "VN", "NG"],
            "Count": [200, 180, 170, 160, 150, 140, 130, 120, 110, 100]
        })
    # Add additional mock data for other queries
    # ...
    return pd.DataFrame()
'''
# Define App Layout with Navigation
app.layout = html.Div([
    html.H1("Ransomware Analysis Dashboard", style={"textAlign": "center"}),
    dcc.Tabs(id="dashboard-tabs", value="dashboard1", children=[
        dcc.Tab(label="Top 10 Targets & Sources", value="dashboard1"),
        dcc.Tab(label="Target & Source Changes", value="dashboard2"),
        dcc.Tab(label="Top 10 Active IPs", value="dashboard3"),
        dcc.Tab(label="Attack Trends", value="dashboard4"),
        dcc.Tab(label="Attacks by Date", value="dashboard5"),
    ]),
    html.Div(id="dashboard-content")
])

# Callback to Render Content for Each Dashboard
@app.callback(
    Output("dashboard-content", "children"),
    Input("dashboard-tabs", "value")
)
def render_dashboard(tab_name):
    if tab_name == "dashboard1":
        return render_dashboard_1()
    elif tab_name == "dashboard2":
        return render_dashboard_2()
    elif tab_name == "dashboard3":
        return render_dashboard_3()
    elif tab_name == "dashboard4":
        return render_dashboard_4()
    elif tab_name == "dashboard5":
        return render_dashboard_5()

def render_dashboard_1():
    # Mock data for demonstration (replace these with your query results)
    top_10_targets = get_query_data("top_10_targets_query")
    top_10_sources = get_query_data("top_10_sources_query")

    # Map visualization for Top 10 Targeted Countries
    fig_targets = px.choropleth(
        top_10_targets,
        locations="Country",  # Column with country codes or names
        locationmode="country names",  # Can be "ISO-3" or "country names"
        color="Count",  # Data to be displayed on the map
        title="Top 10 Targeted Countries",
        color_continuous_scale=px.colors.sequential.Plasma
    )

    # Map visualization for Top 10 Threat Source Countries
    fig_sources = px.choropleth(
        top_10_sources,
        locations="Country",
        locationmode="country names",
        color="Count",
        title="Top 10 Threat Source Countries",
        color_continuous_scale=px.colors.sequential.Viridis
    )

    # Return two maps side-by-side
    return html.Div([
        html.Div([dcc.Graph(figure=fig_targets)], style={"width": "48%", "display": "inline-block"}),
        html.Div([dcc.Graph(figure=fig_sources)], style={"width": "48%", "display": "inline-block"}),
    ])


# Add similar functions for other dashboards (Dashboard 2 to Dashboard 5)
# ...
def render_dashboard_2():
    # Mock data for target and source changes
    target_changes = pd.DataFrame({"Country": ["US", "CN"], "Change": [10, -5]})
    source_changes = pd.DataFrame({"Country": ["RU", "IN"], "Change": [15, -8]})

    fig_target_changes = px.bar(target_changes, x="Country", y="Change", title="Target Country Changes")
    fig_source_changes = px.bar(source_changes, x="Country", y="Change", title="Source Country Changes")

    return html.Div([
        dcc.Graph(figure=fig_target_changes),
        dcc.Graph(figure=fig_source_changes)
    ])

def render_dashboard_3():
    active_ips = pd.DataFrame({
        "IP": ["192.168.0.1", "192.168.0.2", "10.0.0.1", "10.0.0.2", "172.16.0.1"],
        "Activity Count": [500, 400, 300, 200, 100]
    })

    fig_active_ips = px.bar(active_ips, x="IP", y="Activity Count", title="Top 10 Active IPs")

    return html.Div([dcc.Graph(figure=fig_active_ips)])

def render_dashboard_4():
    attack_trends = pd.DataFrame({
        "Time": ["2024-12-01", "2024-12-02", "2024-12-03"],
        "Attacks": [100, 150, 200]
    })

    fig_trends = px.line(attack_trends, x="Time", y="Attacks", title="Attack Trends Over Time")

    return html.Div([dcc.Graph(figure=fig_trends)])

def render_dashboard_5():
    by_day = pd.DataFrame({"Day": ["2024-12-01", "2024-12-02"], "Count": [300, 250]})
    by_month = pd.DataFrame({"Month": ["December"], "Count": [550]})
    by_expiration = pd.DataFrame({"Date": ["2024-12-10"], "Count": [500]})

    fig_day = px.bar(by_day, x="Day", y="Count", title="Attacks by Creation Day")
    fig_month = px.bar(by_month, x="Month", y="Count", title="Attacks by Creation Month")
    fig_expiration = px.bar(by_expiration, x="Date", y="Count", title="Attacks by Expiration Date")

    return html.Div([
        dcc.Graph(figure=fig_day),
        dcc.Graph(figure=fig_month),
        dcc.Graph(figure=fig_expiration)
    ])



# Run the App
if __name__ == "__main__":
    app.run_server(debug=True)
