"""
NEMWEB Price Dashboard - Databricks App

A real-time dashboard for Australian electricity market prices using Dash.
Displays regional reference prices (RRP) across the 5 NEM regions.

Deploy to Databricks:
    databricks apps create nemweb-prices
    databricks apps deploy nemweb-prices --source-code-path /Workspace/Users/.../app
"""

import os
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import dash
from dash import dcc, html, Input, Output, callback
import dash_ag_grid as dag
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Try to import Databricks SQL connector (available in Databricks runtime)
try:
    from databricks import sql as databricks_sql
    DATABRICKS_AVAILABLE = True
except ImportError:
    DATABRICKS_AVAILABLE = False

# Try to import nemweb_dispatch for live data fetching
try:
    import sys
    import os as _os
    # Add src to path for local imports
    _src_path = _os.path.join(_os.path.dirname(_os.path.dirname(__file__)), "src")
    if _src_path not in sys.path:
        sys.path.insert(0, _src_path)
    from nemweb_dispatch import fetch_dispatch_region, fetch_dispatch_interconnector
    NEMWEB_LIVE_AVAILABLE = True
except ImportError:
    NEMWEB_LIVE_AVAILABLE = False

# Configuration from environment variables
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
_host = os.environ.get("DATABRICKS_HOST", "")
# Strip protocol prefix if present (connector expects hostname only)
DATABRICKS_HOST = _host.replace("https://", "").replace("http://", "").rstrip("/")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
# Data source: "sample" (fake), "delta" (from table), or "live" (direct from NEMWEB API)
DATA_SOURCE = os.environ.get("NEMWEB_DATA_SOURCE", "live" if NEMWEB_LIVE_AVAILABLE else "sample")
DELTA_TABLE = os.environ.get("NEMWEB_DELTA_TABLE", "nemweb_prices")
# Number of 5-minute intervals to fetch for live mode (12 = 1 hour)
LIVE_INTERVALS = int(os.environ.get("NEMWEB_LIVE_INTERVALS", "12"))

# NEM Regions
NEM_REGIONS = ["NSW1", "VIC1", "QLD1", "SA1", "TAS1"]

# Region display names and colors
REGION_CONFIG = {
    "NSW1": {"name": "New South Wales", "color": "#1f77b4"},
    "VIC1": {"name": "Victoria", "color": "#ff7f0e"},
    "QLD1": {"name": "Queensland", "color": "#2ca02c"},
    "SA1": {"name": "South Australia", "color": "#d62728"},
    "TAS1": {"name": "Tasmania", "color": "#9467bd"},
}

# Price thresholds for alerts ($/MWh)
PRICE_WARNING = 100
PRICE_ALERT = 300
PRICE_CRITICAL = 1000


def generate_sample_data() -> pd.DataFrame:
    """Generate sample price data for demo purposes."""
    # Note: No fixed seed - data varies on each refresh to simulate live updates

    # Generate 24 hours of 5-minute data
    timestamps = pd.date_range(
        start=datetime.now() - timedelta(hours=24),
        end=datetime.now(),
        freq="5min"
    )

    data = []
    for ts in timestamps:
        hour = ts.hour
        # Simulate daily price pattern (higher during peak hours)
        base_price = 50 + 30 * np.sin((hour - 6) * np.pi / 12)

        for region in NEM_REGIONS:
            # Add regional variation
            regional_factor = {
                "NSW1": 1.0,
                "VIC1": 0.95,
                "QLD1": 1.1,
                "SA1": 1.2,  # SA often has higher prices
                "TAS1": 0.85,
            }[region]

            # Add randomness and occasional spikes
            noise = np.random.normal(0, 15)
            spike = 0
            if np.random.random() < 0.02:  # 2% chance of price spike
                spike = np.random.exponential(200)

            price = max(0, base_price * regional_factor + noise + spike)

            # Demand varies by region
            base_demand = {"NSW1": 8000, "VIC1": 5500, "QLD1": 6500, "SA1": 2000, "TAS1": 1200}
            demand = base_demand[region] * (0.8 + 0.4 * np.sin((hour - 8) * np.pi / 12))
            demand += np.random.normal(0, 200)

            data.append({
                "timestamp": ts,
                "region": region,
                "rrp": round(price, 2),
                "demand_mw": round(demand, 1),
            })

    return pd.DataFrame(data)


def fetch_live_data() -> Optional[pd.DataFrame]:
    """Fetch live price data directly from NEMWEB API."""
    if not NEMWEB_LIVE_AVAILABLE:
        return None

    try:
        # Fetch recent dispatch data (each file = 5 minutes)
        data = fetch_dispatch_region(max_files=LIVE_INTERVALS, debug=False)

        if not data:
            return None

        # Convert to DataFrame with expected column names
        rows = []
        for row in data:
            rows.append({
                "timestamp": row.get("SETTLEMENTDATE"),
                "region": row.get("REGIONID"),
                "rrp": row.get("RRP"),
                "demand_mw": row.get("TOTALDEMAND"),
            })

        df = pd.DataFrame(rows)
        return df

    except Exception as e:
        print(f"Error fetching live NEMWEB data: {e}")
        return None


def fetch_delta_data() -> Optional[pd.DataFrame]:
    """Fetch price data from Delta table via Databricks SQL."""
    if not DATABRICKS_AVAILABLE or not all([WAREHOUSE_ID, DATABRICKS_HOST, DATABRICKS_TOKEN]):
        return None

    try:
        with databricks_sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
            access_token=DATABRICKS_TOKEN
        ) as conn:
            with conn.cursor() as cursor:
                # Query works with both DISPATCHREGIONSUM (has TOTALDEMAND)
                # and DISPATCHPRICE (has RRP) schemas
                query = f"""
                    SELECT
                        SETTLEMENTDATE as timestamp,
                        REGIONID as region,
                        COALESCE(RRP, 0) as rrp,
                        COALESCE(TOTALDEMAND, 0) as demand_mw
                    FROM {DELTA_TABLE}
                    WHERE SETTLEMENTDATE >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
                    ORDER BY SETTLEMENTDATE DESC
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=columns)
                return df
    except Exception as e:
        print(f"Error fetching Delta data: {e}")
        return None


def get_price_data() -> pd.DataFrame:
    """Get price data from configured source."""
    if DATA_SOURCE == "live":
        df = fetch_live_data()
        if df is not None:
            print(f"[LIVE] Fetched {len(df)} rows from NEMWEB")
            return df
        print("[LIVE] Failed to fetch, falling back to sample")

    if DATA_SOURCE == "delta":
        df = fetch_delta_data()
        if df is not None:
            return df

    # Fall back to sample data
    return generate_sample_data()


def get_price_status(price: float) -> tuple[str, str]:
    """Return status label and color based on price."""
    if price >= PRICE_CRITICAL:
        return "CRITICAL", "#dc3545"
    elif price >= PRICE_ALERT:
        return "HIGH", "#fd7e14"
    elif price >= PRICE_WARNING:
        return "ELEVATED", "#ffc107"
    else:
        return "NORMAL", "#28a745"


# Initialize Dash app
app = dash.Dash(
    __name__,
    title="NEMWEB Price Dashboard",
    update_title="Updating...",
    suppress_callback_exceptions=True,
)

# For Databricks Apps deployment
server = app.server

# App layout
app.layout = html.Div([
    # Header
    html.Div([
        html.H1("NEM Electricity Prices", style={"margin": 0}),
        html.P(
            "Real-time spot prices across the Australian National Electricity Market",
            style={"margin": "5px 0 0 0", "opacity": 0.8}
        ),
    ], style={
        "backgroundColor": "#1a1a2e",
        "color": "white",
        "padding": "20px",
        "marginBottom": "20px",
    }),

    # Current prices cards
    html.Div([
        html.H3("Current Prices ($/MWh)", style={"marginBottom": "15px"}),
        html.Div(id="price-cards", style={
            "display": "flex",
            "flexWrap": "wrap",
            "gap": "15px",
        }),
    ], style={"padding": "0 20px", "marginBottom": "20px"}),

    # Controls
    html.Div([
        html.Label("Time Range:", style={"marginRight": "10px", "fontWeight": "bold"}),
        dcc.Dropdown(
            id="time-range",
            options=[
                {"label": "Last 1 Hour", "value": 1},
                {"label": "Last 6 Hours", "value": 6},
                {"label": "Last 12 Hours", "value": 12},
                {"label": "Last 24 Hours", "value": 24},
            ],
            value=6,
            style={"width": "200px", "display": "inline-block"},
            clearable=False,
        ),
        html.Label("Regions:", style={"marginLeft": "30px", "marginRight": "10px", "fontWeight": "bold"}),
        dcc.Dropdown(
            id="region-filter",
            options=[{"label": REGION_CONFIG[r]["name"], "value": r} for r in NEM_REGIONS],
            value=NEM_REGIONS,
            multi=True,
            style={"width": "400px", "display": "inline-block"},
        ),
    ], style={"padding": "0 20px", "marginBottom": "20px"}),

    # Price chart
    html.Div([
        html.H3("Price History", style={"marginBottom": "10px"}),
        dcc.Graph(id="price-chart", style={"height": "400px"}),
    ], style={"padding": "0 20px", "marginBottom": "20px"}),

    # Price and demand comparison
    html.Div([
        html.Div([
            html.H3("Demand by Region", style={"marginBottom": "10px"}),
            dcc.Graph(id="demand-chart", style={"height": "300px"}),
        ], style={"flex": 1}),
        html.Div([
            html.H3("Price Distribution", style={"marginBottom": "10px"}),
            dcc.Graph(id="price-distribution", style={"height": "300px"}),
        ], style={"flex": 1}),
    ], style={"display": "flex", "gap": "20px", "padding": "0 20px", "marginBottom": "20px"}),

    # Data table
    html.Div([
        html.H3("Recent Prices", style={"marginBottom": "10px"}),
        dag.AgGrid(
            id="price-table",
            columnDefs=[
                {"field": "timestamp", "headerName": "Time", "sortable": True, "filter": True},
                {"field": "region", "headerName": "Region", "sortable": True, "filter": True},
                {
                    "field": "rrp",
                    "headerName": "Price ($/MWh)",
                    "sortable": True,
                    "filter": "agNumberColumnFilter",
                    "valueFormatter": {"function": "d3.format(',.2f')(params.value)"},
                    "cellStyle": {
                        "styleConditions": [
                            {"condition": f"params.value >= {PRICE_CRITICAL}", "style": {"backgroundColor": "#dc354520", "color": "#dc3545", "fontWeight": "bold"}},
                            {"condition": f"params.value >= {PRICE_ALERT}", "style": {"backgroundColor": "#fd7e1420", "color": "#fd7e14"}},
                            {"condition": f"params.value >= {PRICE_WARNING}", "style": {"backgroundColor": "#ffc10720"}},
                        ]
                    }
                },
                {
                    "field": "demand_mw",
                    "headerName": "Demand (MW)",
                    "sortable": True,
                    "filter": "agNumberColumnFilter",
                    "valueFormatter": {"function": "d3.format(',.0f')(params.value)"},
                },
            ],
            defaultColDef={"resizable": True, "minWidth": 100},
            dashGridOptions={"pagination": True, "paginationPageSize": 20},
            style={"height": "400px"},
        ),
    ], style={"padding": "0 20px", "marginBottom": "20px"}),

    # Auto-refresh
    dcc.Interval(
        id="refresh-interval",
        interval=30 * 1000,  # 30 seconds
        n_intervals=0,
    ),

    # Data store
    dcc.Store(id="price-data"),

    # Footer
    html.Div([
        html.P([
            "Data source: ",
            html.A("AEMO NEMWEB", href="https://www.nemweb.com.au", target="_blank"),
            " | Prices in AUD per MWh | ",
            html.Span(id="last-updated"),
        ], style={"margin": 0, "opacity": 0.7}),
    ], style={
        "backgroundColor": "#f5f5f5",
        "padding": "15px 20px",
        "textAlign": "center",
        "fontSize": "14px",
    }),
])


@callback(
    Output("price-data", "data"),
    Input("refresh-interval", "n_intervals"),
)
def refresh_data(n):
    """Fetch fresh price data."""
    df = get_price_data()
    return df.to_json(date_format="iso", orient="split")


@callback(
    Output("price-cards", "children"),
    Output("last-updated", "children"),
    Input("price-data", "data"),
)
def update_price_cards(data):
    """Update the current price cards."""
    if not data:
        return [], "No data"

    df = pd.read_json(data, orient="split")

    # Handle empty DataFrame
    if df.empty:
        return [], "No data available"

    # Get latest price for each region
    latest = df.sort_values("timestamp").groupby("region").last().reset_index()

    cards = []
    for _, row in latest.iterrows():
        region = row["region"]
        price = row["rrp"]
        demand = row["demand_mw"]
        status, color = get_price_status(price)

        # Get region color, with fallback for unknown regions
        region_color = REGION_CONFIG.get(region, {}).get("color", "#6c757d")

        card = html.Div([
            html.Div([
                html.Span(region, style={"fontWeight": "bold", "fontSize": "18px"}),
                html.Span(
                    status,
                    style={
                        "backgroundColor": color,
                        "color": "white",
                        "padding": "2px 8px",
                        "borderRadius": "4px",
                        "fontSize": "12px",
                        "marginLeft": "10px",
                    }
                ),
            ]),
            html.Div(
                f"${price:,.2f}",
                style={
                    "fontSize": "32px",
                    "fontWeight": "bold",
                    "color": color if price >= PRICE_WARNING else "#333",
                }
            ),
            html.Div(
                f"{demand:,.0f} MW",
                style={"opacity": 0.7, "fontSize": "14px"}
            ),
        ], style={
            "backgroundColor": "white",
            "border": f"2px solid {region_color}",
            "borderRadius": "8px",
            "padding": "15px",
            "minWidth": "150px",
            "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
        })
        cards.append(card)

    last_time = df["timestamp"].max()
    if isinstance(last_time, str):
        last_time = pd.to_datetime(last_time)

    # Handle NaT (empty timestamps)
    if pd.isna(last_time):
        return cards, "Last updated: Unknown"

    return cards, f"Last updated: {last_time.strftime('%Y-%m-%d %H:%M:%S')}"


@callback(
    Output("price-chart", "figure"),
    Input("price-data", "data"),
    Input("time-range", "value"),
    Input("region-filter", "value"),
)
def update_price_chart(data, hours, regions):
    """Update the price history line chart."""
    if not data or not regions:
        return go.Figure()

    df = pd.read_json(data, orient="split")
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Filter by time range
    cutoff = datetime.now() - timedelta(hours=hours)
    df = df[df["timestamp"] >= cutoff]

    # Filter by regions
    df = df[df["region"].isin(regions)]

    fig = px.line(
        df,
        x="timestamp",
        y="rrp",
        color="region",
        color_discrete_map={r: REGION_CONFIG[r]["color"] for r in NEM_REGIONS},
        labels={"rrp": "Price ($/MWh)", "timestamp": "Time", "region": "Region"},
    )

    # Add threshold lines
    fig.add_hline(y=PRICE_WARNING, line_dash="dash", line_color="orange", opacity=0.5,
                  annotation_text="Warning")
    fig.add_hline(y=PRICE_ALERT, line_dash="dash", line_color="red", opacity=0.5,
                  annotation_text="Alert")

    fig.update_layout(
        margin=dict(l=50, r=20, t=20, b=50),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )

    return fig


@callback(
    Output("demand-chart", "figure"),
    Input("price-data", "data"),
    Input("region-filter", "value"),
)
def update_demand_chart(data, regions):
    """Update the demand bar chart."""
    if not data or not regions:
        return go.Figure()

    df = pd.read_json(data, orient="split")

    # Get latest demand for each region
    latest = df.sort_values("timestamp").groupby("region").last().reset_index()
    latest = latest[latest["region"].isin(regions)]

    fig = px.bar(
        latest,
        x="region",
        y="demand_mw",
        color="region",
        color_discrete_map={r: REGION_CONFIG[r]["color"] for r in NEM_REGIONS},
        labels={"demand_mw": "Demand (MW)", "region": "Region"},
    )

    fig.update_layout(
        margin=dict(l=50, r=20, t=20, b=50),
        showlegend=False,
    )

    return fig


@callback(
    Output("price-distribution", "figure"),
    Input("price-data", "data"),
    Input("time-range", "value"),
    Input("region-filter", "value"),
)
def update_price_distribution(data, hours, regions):
    """Update the price distribution box plot."""
    if not data or not regions:
        return go.Figure()

    df = pd.read_json(data, orient="split")
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Filter by time range
    cutoff = datetime.now() - timedelta(hours=hours)
    df = df[df["timestamp"] >= cutoff]

    # Filter by regions
    df = df[df["region"].isin(regions)]

    fig = px.box(
        df,
        x="region",
        y="rrp",
        color="region",
        color_discrete_map={r: REGION_CONFIG[r]["color"] for r in NEM_REGIONS},
        labels={"rrp": "Price ($/MWh)", "region": "Region"},
    )

    fig.update_layout(
        margin=dict(l=50, r=20, t=20, b=50),
        showlegend=False,
    )

    return fig


@callback(
    Output("price-table", "rowData"),
    Input("price-data", "data"),
    Input("time-range", "value"),
    Input("region-filter", "value"),
)
def update_price_table(data, hours, regions):
    """Update the price data table."""
    if not data or not regions:
        return []

    df = pd.read_json(data, orient="split")
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Filter by time range
    cutoff = datetime.now() - timedelta(hours=hours)
    df = df[df["timestamp"] >= cutoff]

    # Filter by regions
    df = df[df["region"].isin(regions)]

    # Sort by timestamp descending
    df = df.sort_values("timestamp", ascending=False)

    # Format timestamp for display
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M")

    return df.to_dict("records")


if __name__ == "__main__":
    # Run locally for development
    app.run(debug=True, host="0.0.0.0", port=8050)
