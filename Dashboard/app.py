from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
from influxdb_client import InfluxDBClient

# Create Dash App
app = Dash(__name__)
app.title = "Ransomware Dashboard"


# Connection details
url = "http://localhost:8086"  # Replace with your InfluxDB URL
token = 'JkLVh_Glxl0FfIHnJM3C8HZOVvY_kG_spqDAJ4yK2HlhH7ia6oQqLf5IOy2XpvzMVlThyoFVjiAfsztM_CE8vw=='   # Replace with your token
org = "ransomeware"      # Replace with your organization
bucket = 'ransomware'
PREDICTION_BUCKET = 'prediction'



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
    if query_name == "top_10_targets_per_country":
        query = '''
        from(bucket: "ransomware")
        |> range(start: -5y)  // Adjust the time range as needed
        |> filter(fn: (r) => r._measurement == "top_10_target_countries")
        |> group(columns: ["target_country"])
        |> sum(column: "_value")  // Sum the attack counts for each target_country
        |> sort(columns: ["_value"], desc: true)
        |> limit(n: 10)
        '''
        influx_data = fetch_influxdb_data(query)
        print("Fetched data from InfluxDB:")
        print(influx_data.head(10))
        return influx_data.rename(columns={"target_country": "Country", "_value": "Attack Count"})
    

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
    try:
        # Fetch data for top 10 targets
        top_10_targets = get_query_data("top_10_targets_per_country")
        
        # Validate if the DataFrame is not empty and contains required columns
        if top_10_targets.empty:
            raise ValueError("No data available for top 10 targets.")

        if not all(col in top_10_targets.columns for col in ["Country", "Attack Count"]):
            raise ValueError("Missing required columns: 'Country' and 'Attack Count' in the data.")

        # Create a choropleth map
        fig_choropleth = px.choropleth(
            top_10_targets,
            locations="Country",
            locationmode="country names",  # Ensure that country names match Plotly's format
            color="Attack Count",
            hover_name="Country",
            title="Top 10 Target Countries by Attack Count",
            color_continuous_scale=px.colors.sequential.Plasma
        )

        # Adjust layout
        fig_choropleth.update_layout(
            geo=dict(
                showframe=False,
                showcoastlines=True,
                projection_type="natural earth"
            ),
            margin={"r": 0, "t": 40, "l": 0, "b": 0}
        )

        # Return the choropleth map
        return html.Div([
            html.Div([dcc.Graph(figure=fig_choropleth)], style={"width": "100%", "display": "block"}),
        ])
    except ValueError as e:
        # Handle specific errors gracefully
        return html.Div([
            html.H3("Error in Rendering Dashboard", style={"color": "red"}),
            html.P(str(e))
        ])
    except Exception as e:
        # Catch-all for unexpected errors
        return html.Div([
            html.H3("Unexpected Error", style={"color": "red"}),
            html.P(str(e))
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
