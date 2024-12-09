from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
from influxdb_client import InfluxDBClient
import os
# Create Dash App
app = Dash(__name__)
app.title = "Ransomware Dashboard"


# Connection details
url = os.getenv("INFLUXDB_HOST", "http://localhost:8086")
token = '1j5Ug_kxAwwAoAAblcCWBX9Kmo7nDiKhBEFA7gsctb5td2E86mm4stX9hb9ozOhHEKjckc8H0NZVniXIdZMU0w=='  # Replace with your token
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

def fetch_clustering_data():
    query = '''
    from(bucket: "prediction")
        |> range(start: -1y)
        |> filter(fn: (r) => r._measurement == "indicator_predictions")
        |> group(columns: ["indicator", "cluster"])
        |> sort(columns: ["_time"], desc: true)
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    data = fetch_influxdb_data(query)
    
    # Rename columns if necessary
    if "_time" in data.columns:
        data = data.rename(columns={"_time": "Timestamp"})
        data["Timestamp"] = pd.to_datetime(data["Timestamp"])
    
    print("Fetched Clustering Data Preview:")
    print(data.head())
    return data


def fetch_prediction_data():
    query = '''
    from(bucket: "forcast")
        |> range(start: -1y)
        |> filter(fn: (r) => r._measurement == "indicator_predictions")
        |> sort(columns: ["_time"], desc: true)
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    data = fetch_influxdb_data(query)
    data = data.rename(columns={
        "_time": "Timestamp",
        "num_attacks": "Number of Attacks"
    })
    data["Timestamp"] = pd.to_datetime(data["Timestamp"])
    return data



def fetch_target_country_changes():
    query = '''
    from(bucket: "ransomware")
        |> range(start: -1y)
        |> filter(fn: (r) => r._measurement == "detect_target_country_changes")
        |> group(columns: ["target_country", "window"])
        |> sort(columns: ["_time"], desc: false)
    '''
    influx_data = fetch_influxdb_data(query)
    influx_data = influx_data.rename(columns={"_value": "Change Count", "target_country": "Country", "_time": "Timestamp"})
    influx_data["Timestamp"] = pd.to_datetime(influx_data["Timestamp"])
    print("Target Country Changes Data:")
    print(influx_data.head())
    return influx_data

def fetch_source_country_changes():
    query = '''
    from(bucket: "ransomware")
        |> range(start: -1y)
        |> filter(fn: (r) => r._measurement == "detect_source_country_changes")
        |> group(columns: ["source_country", "window"])
        |> sort(columns: ["_time"], desc: false)
    '''
    influx_data = fetch_influxdb_data(query)
    influx_data = influx_data.rename(columns={"_value": "Change Count", "source_country": "Country", "_time": "Timestamp"})
    influx_data["Timestamp"] = pd.to_datetime(influx_data["Timestamp"])
    print("Source Country Changes Data:")
    print(influx_data.head())
    return influx_data


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
    elif query_name == "top_10_threat_sources":
        query = '''
        from(bucket: "ransomware")
            |> range(start: -1y)
            |> filter(fn: (r) => r._measurement == "top_10_source_countries")
            |> group(columns: ["source_country"])
            |> sort(columns: ["_value"], desc: true)
            |> limit(n: 10)
        '''
        influx_data = fetch_influxdb_data(query)
        print("Fetched data for top 10 threat sources:")
        print(influx_data.head(10))
        return influx_data.rename(columns={"source_country": "Country", "_value": "Attack Count"})

    elif query_name == "target_country_changes":
        # Query for target country changes
        query = '''
        from(bucket: "ransomware")
            |> range(start: -5y)
            |> filter(fn: (r) => r._measurement == "detect_target_country_changes")
            |> group(columns: ["target_country", "window"])
            |> sort(columns: ["_time"], desc: false)
        '''
        influx_data = fetch_influxdb_data(query)
        influx_data = influx_data.rename(columns={"_value": "Change Count", "target_country": "Country", "_time": "Timestamp"})
        influx_data["Timestamp"] = pd.to_datetime(influx_data["Timestamp"])
        return influx_data

    elif query_name == "source_country_changes":
        # Query for source country changes
        query = '''
        from(bucket: "ransomware")
            |> range(start: -5y)
            |> filter(fn: (r) => r._measurement == "detect_source_country_changes")
            |> group(columns: ["source_country", "window"])
            |> sort(columns: ["_time"], desc: false)
        '''
        influx_data = fetch_influxdb_data(query)
        influx_data = influx_data.rename(columns={"_value": "Change Count", "source_country": "Country", "_time": "Timestamp"})
        influx_data["Timestamp"] = pd.to_datetime(influx_data["Timestamp"])
        return influx_data
    elif query_name == "top_10_active_ips":
        # Query for top 10 active IPs
        query = '''
        from(bucket: "ransomware")
            |> range(start: -1y)
            |> filter(fn: (r) => r._measurement == "top_10_active_ips")
            |> group(columns: ["ip"])
            |> sort(columns: ["_value"], desc: true)
            |> limit(n: 10)
        '''
        influx_data = fetch_influxdb_data(query)
        influx_data = influx_data.rename(columns={"ip": "IP Address", "_value": "Attack Count"})
        return influx_data

    elif query_name == "top_attack_type":
        # Query for top attack type
        query = '''
        from(bucket: "ransomware")
            |> range(start: -1y)
            |> filter(fn: (r) => r._measurement == "top_attack_type")
            |> group(columns: ["type"])
            |> sort(columns: ["_value"], desc: true)
            |> limit(n: 1)
        '''
        influx_data = fetch_influxdb_data(query)
        influx_data = influx_data.rename(columns={"type": "Attack Type", "_value": "Attack Count"})
        return influx_data
    elif query_name == "top_10_authors":
        # Define InfluxDB query to get top 10 authors
        query = '''
        from(bucket: "ransomware")
            |> range(start: -1y)  // Adjust the time range as needed
            |> filter(fn: (r) => r._measurement == "top_10_authors")
            |> group(columns: ["author_name"])
            |> sort(columns: ["_value"], desc: true)
            |> limit(n: 10)
        '''
        # Fetch the data from InfluxDB
        influx_data = fetch_influxdb_data(query)
        # Rename columns to match visualization needs
        influx_data = influx_data.rename(columns={"author_name": "Author Name", "_value": "Indicator Count"})
        return influx_data

    elif query_name == "top_10_cities":
        # Define InfluxDB query to get top 10 cities
        query = '''
        from(bucket: "ransomware")
            |> range(start: -1y)  // Adjust the time range as needed
            |> filter(fn: (r) => r._measurement == "top_10_cities")
            |> group(columns: ["source_city"])
            |> sort(columns: ["_value"], desc: true)
            |> limit(n: 10)
        '''
        # Fetch the data from InfluxDB
        influx_data = fetch_influxdb_data(query)
        # Rename columns to match visualization needs
        influx_data = influx_data.rename(columns={"source_city": "City", "_value": "Attack Count"})
        return influx_data
    elif query_name == "attacks_by_creation_day":
        query = '''
        from(bucket: "ransomware")
            |> range(start: -5y)  // Adjust time range as needed
            |> filter(fn: (r) => r._measurement == "attack_by_creation_day")
            |> group(columns: ["created_date"])
            |> aggregateWindow(every: 1d, fn: sum, createEmpty: false)
            |> sort(columns: ["_time"], desc: false)
        '''
        influx_data = fetch_influxdb_data(query)
        influx_data = influx_data.rename(columns={"_time": "Date", "_value": "Attack Count"})
        return influx_data

    elif query_name == "attacks_by_creation_month":
        query = '''
        from(bucket: "ransomware")
            |> range(start: -5y)  // Adjust time range as needed
            |> filter(fn: (r) => r._measurement == "attacks_by_creation_month")
            |> group(columns: ["year", "month"])
            |> sort(columns: ["year", "month"], desc: false)
        '''
        influx_data = fetch_influxdb_data(query)
        influx_data["Year-Month"] = influx_data["year"].astype(str) + "-" + influx_data["month"].astype(str).str.zfill(2)
        influx_data = influx_data.rename(columns={"_value": "Attack Count"})
        return influx_data

    elif query_name == "predictions":
        query = '''
        from(bucket: "prediction")
            |> range(start: -8y)  // Adjust the time range as needed
            |> filter(fn: (r) => r._measurement == "indicator_predictions")
            |> limit(n: 100)
        '''
        influx_data = fetch_influxdb_data(query)

        # Ensure the data is converted to a Pandas DataFrame
        if isinstance(influx_data, list):
            influx_data = pd.DataFrame(influx_data)

        # Check if the DataFrame is empty
        if influx_data.empty:
            print("No prediction data available.")
            return pd.DataFrame()

        # Rename columns
        influx_data = influx_data.rename(columns={
            "_time": "Timestamp",
            "num_attacks": "Number of Attacks"
        })

        # Convert Timestamp to datetime
        influx_data["Timestamp"] = pd.to_datetime(influx_data["Timestamp"], errors='coerce')

        # Drop rows with invalid timestamps
        influx_data = influx_data.dropna(subset=["Timestamp"])

        return influx_data

    
    else:
        raise ValueError(f"Unknown query_name: {query_name}")

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
        dcc.Tab(label="Clustering Results", value="dashboard6"),
        dcc.Tab(label="Prediction Results", value="dashboard7")
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
    elif tab_name == "dashboard6":
        return render_dashboard_6()
    elif tab_name == "dashboard7":
        return render_dashboard_7()

# Dashboard 6: Clustering Results
def render_dashboard_6():
    # Fetch clustering data
    clustering_data = get_query_data("predictions")

    # Ensure required columns exist
    #if clustering_data.empty or not {"indicator", "cluster"}.issubset(clustering_data.columns):
    #return html.Div("No clustering data available.", style={"textAlign": "center", "color": "red"})

    # Create scatter plot
    fig_scatter = px.scatter(
        clustering_data,
        x="source_latitude",
        y="source_longitude",
        color="cluster",
        hover_name="indicator",
        title="Clustering Results: Indicators by Cluster",
        labels={"cluster": "Cluster"},
        color_discrete_sequence=px.colors.qualitative.Set1
    )

    # Adjust layout
    fig_scatter.update_layout(
        xaxis_title="Source Latitude",
        yaxis_title="Source Longitude",
        margin={"r": 0, "t": 40, "l": 0, "b": 0},
        legend_title="Cluster"
    )

    # Return scatter plot
    return html.Div([
        html.H3("Clustering Dashboard: Indicator Clusters", style={"textAlign": "center"}),
        dcc.Graph(figure=fig_scatter)
    ])
    
# Dashboard 7: Prediction Results
def render_dashboard_7():
    prediction_data = fetch_prediction_data()
    fig_predictions = px.line(
        prediction_data,
        x="Timestamp",
        y="Number of Attacks",
        title="Prediction Results Over Time",
        labels={"Number of Attacks": "Predicted Number of Attacks", "Timestamp": "Time"}
    )
    return html.Div([
        html.Div([dcc.Graph(figure=fig_predictions)], style={"width": "100%", "display": "block"}),
    ])
    
def render_dashboard_1():
    # Fetch data for top 10 targets
    top_10_targets = get_query_data("top_10_targets_per_country")
    # Fetch data for top 10 threat sources
    top_10_sources = get_query_data("top_10_threat_sources")

    # Create a choropleth map for targets
    fig_choropleth_targets = px.choropleth(
        top_10_targets,
        locations="Country",
        locationmode="country names",
        color="Attack Count",
        hover_name="Country",
        title="Top 10 Target Countries by Attack Count",
        color_continuous_scale=px.colors.sequential.Plasma
    )

    # Adjust layout for targets map
    fig_choropleth_targets.update_layout(
        geo=dict(
            showframe=False,
            showcoastlines=True,
            projection_type="natural earth"
        ),
        margin={"r": 0, "t": 40, "l": 0, "b": 0}
    )

    # Create a choropleth map for sources
    fig_choropleth_sources = px.choropleth(
        top_10_sources,
        locations="Country",
        locationmode="country names",
        color="Attack Count",
        hover_name="Country",
        title="Top 10 Source Countries by Attack Count",
        color_continuous_scale=px.colors.sequential.Sunset
    )

    # Adjust layout for sources map
    fig_choropleth_sources.update_layout(
        geo=dict(
            showframe=False,
            showcoastlines=True,
            projection_type="natural earth"
        ),
        margin={"r": 0, "t": 40, "l": 0, "b": 0}
    )

    # Return both choropleth maps
    return html.Div([
        html.Div([dcc.Graph(figure=fig_choropleth_targets)], style={"width": "100%", "display": "block"}),
        html.Div([dcc.Graph(figure=fig_choropleth_sources)], style={"width": "100%", "display": "block"}),
    ])



# Add similar functions for other dashboards (Dashboard 2 to Dashboard 5)
# ...
def render_dashboard_2():
    # Fetch data
    target_changes = fetch_target_country_changes()
    source_changes = fetch_source_country_changes()

    # Create line chart for target country changes
    fig_target_changes = px.line(
        target_changes,
        x="Timestamp",
        y="Change Count",
        color="Country",
        title="Target Country Changes Over Time",
        labels={"Change Count": "Change Count", "Timestamp": "Time"}
    )

    # Create line chart for source country changes
    fig_source_changes = px.line(
        source_changes,
        x="Timestamp",
        y="Change Count",
        color="Country",
        title="Source Country Changes Over Time",
        labels={"Change Count": "Change Count", "Timestamp": "Time"}
    )

    # Return the dashboard layout
    return html.Div([
        html.Div([dcc.Graph(figure=fig_target_changes)], style={"width": "100%", "display": "block", "margin-bottom": "40px"}),
        html.Div([dcc.Graph(figure=fig_source_changes)], style={"width": "100%", "display": "block"}),
    ])


def render_dashboard_3():
    # Fetch data for top 10 active IPs
    top_10_ips = get_query_data("top_10_active_ips")

    # Fetch data for top attack type
    top_attack_type = get_query_data("top_attack_type")

    # Create bar chart for top 10 active IPs
    fig_ips = px.bar(
        top_10_ips,
        x="IP Address",
        y="Attack Count",
        title="Top 10 Active IPs by Attack Count",
        labels={"Attack Count": "Number of Attacks", "IP Address": "IP Address"},
        color="Attack Count",
        color_continuous_scale=px.colors.sequential.Viridis
    )
    fig_ips.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})

    # Create pie chart for top attack type
    fig_attack_type = px.pie(
        top_attack_type,
        names="Attack Type",
        values="Attack Count",
        title="Top Attack Type",
        color_discrete_sequence=["blue", "red"]
    )
    fig_attack_type.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})

    # Return the dashboard layout
    return html.Div([
        html.Div([
            html.H3("Dashboard 3: Active IPs and Attack Types", style={"textAlign": "center"}),
            dcc.Graph(figure=fig_ips),
            dcc.Graph(figure=fig_attack_type)
        ], style={"width": "100%", "display": "block"}),
    ])


def render_dashboard_4():
    # Fetch data for top 10 authors
    top_10_authors = get_query_data("top_10_authors")

    # Fetch data for top 10 cities
    top_10_cities = get_query_data("top_10_cities")

    # Create bar chart for top 10 authors
    fig_authors = px.bar(
        top_10_authors,
        x="Author Name",
        y="Indicator Count",
        title="Top 10 Authors by Indicator Count",
        labels={"Indicator Count": "Count", "Author Name": "Author Name"},
        color="Indicator Count",
        color_continuous_scale=px.colors.sequential.Blues
    )
    fig_authors.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})

    # Create bar chart for top 10 cities
    fig_cities = px.bar(
        top_10_cities,
        x="City",
        y="Attack Count",
        title="Top 10 Cities by Attack Count",
        labels={"Attack Count": "Number of Attacks", "City": "City"},
        color="Attack Count",
        color_continuous_scale=px.colors.sequential.Reds
    )
    fig_cities.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})

    # Return the dashboard layout
    return html.Div([
        html.Div([
            html.H3("Dashboard 4: Top Authors and Cities", style={"textAlign": "center"}),
            dcc.Graph(figure=fig_authors),
            dcc.Graph(figure=fig_cities)
        ], style={"width": "100%", "display": "block"}),
    ])


def render_dashboard_5():
    # Fetch data for daily attacks
    daily_attacks = get_query_data("attacks_by_creation_day")
    
    # Fetch data for monthly attacks
    monthly_attacks = get_query_data("attacks_by_creation_month")

    # Create a line chart for daily attacks
    fig_daily = px.line(
        daily_attacks,
        x="Date",
        y="Attack Count",
        title="Daily Attacks Over Time",
        labels={"Attack Count": "Number of Attacks", "Date": "Date"}
    )
    fig_daily.update_traces(line=dict(color="blue"))

    # Create a bar chart for monthly attacks
    fig_monthly = px.bar(
        monthly_attacks,
        x="Year-Month",
        y="Attack Count",
        title="Monthly Attacks Over Time",
        labels={"Attack Count": "Number of Attacks", "Year-Month": "Month-Year"}
    )
    fig_monthly.update_traces(marker_color="green")

    # Return the layout with both charts
    return html.Div([
        html.Div([
            html.H3("Attack Trends Dashboard"),
            dcc.Graph(figure=fig_daily),
            dcc.Graph(figure=fig_monthly),
        ], style={"width": "100%", "display": "block"}),
    ])




# Run the App
if __name__ == "__main__":
    app.run_server(debug=True)
