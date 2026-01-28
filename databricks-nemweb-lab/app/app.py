"""
NEMWEB Price Dashboard - Databricks App

A real-time dashboard for Australian electricity market prices using Dash.
Displays regional reference prices (RRP) across the 5 NEM regions.

Deploy to Databricks:
    databricks apps create nemweb-prices
    databricks apps deploy nemweb-prices --source-code-path /Workspace/Users/.../app
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

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
DATA_SOURCE = os.environ.get("NEMWEB_DATA_SOURCE", "delta")
DELTA_TABLE = os.environ.get("NEMWEB_DELTA_TABLE", "agl.nemweb.dispatch_region")
# Number of 5-minute intervals to fetch for live mode (12 = 1 hour)
LIVE_INTERVALS = int(os.environ.get("NEMWEB_LIVE_INTERVALS", "12"))

# Timezone for display (NEM operates on Australian Eastern Time)
DISPLAY_TZ = ZoneInfo("Australia/Sydney")

# NEM Regions
NEM_REGIONS = ["NSW1", "VIC1", "QLD1", "SA1", "TAS1"]

# AGL Brand Colors
AGL_ORANGE = "#FF6900"
AGL_DARK = "#1A1A1A"
AGL_GREY = "#4A4A4A"
AGL_LIGHT_GREY = "#F5F5F5"

# Region display names and colors (AGL-themed palette)
REGION_CONFIG = {
    "NSW1": {"name": "New South Wales", "color": "#FF6900"},  # AGL Orange
    "VIC1": {"name": "Victoria", "color": "#00A9E0"},  # Blue
    "QLD1": {"name": "Queensland", "color": "#7AB800"},  # Green
    "SA1": {"name": "South Australia", "color": "#E4002B"},  # Red
    "TAS1": {"name": "Tasmania", "color": "#6D2077"},  # Purple
}

# Price thresholds for alerts ($/MWh)
PRICE_WARNING = 100
PRICE_ALERT = 300
PRICE_CRITICAL = 1000


def to_aedt(dt) -> datetime:
    """Convert datetime to Australian Eastern Time for display."""
    if dt is None or pd.isna(dt):
        return None
    if isinstance(dt, str):
        dt = pd.to_datetime(dt)
    if dt.tzinfo is None:
        # Assume UTC if no timezone
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(DISPLAY_TZ)


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


# AGL Logo URL (from their website)
AGL_LOGO_URL = "https://www.agl.com.au/-/media/aglmedia/images/logos/agl-logo.png"

# Initialize Dash app with AGL styling
app = dash.Dash(
    __name__,
    title="AGL NEM Price Monitor",
    update_title="Updating...",
    suppress_callback_exceptions=True,
    external_stylesheets=[
        # Google Fonts - Montserrat (similar to AGL's brand font)
        "https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;500;600;700&display=swap"
    ],
)

# Apply global font styling
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            * {
                font-family: 'Montserrat', -apple-system, BlinkMacSystemFont, sans-serif;
            }
            body {
                margin: 0;
                background-color: #FAFAFA;
            }
            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.7; }
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# For Databricks Apps deployment
server = app.server

# Data source display name
DATA_SOURCE_LABELS = {
    "live": ("LIVE", "Direct from NEMWEB API", AGL_ORANGE),
    "delta": ("LIVE", f"Streaming from Delta Lake", AGL_ORANGE),
    "sample": ("DEMO", "Sample data", AGL_GREY),
}
_source_label, _source_desc, _source_color = DATA_SOURCE_LABELS.get(DATA_SOURCE, ("", "", AGL_GREY))

# App layout
app.layout = html.Div([
    # Header with AGL branding
    html.Div([
        html.Div([
            html.Img(
                src=AGL_LOGO_URL,
                style={"height": "40px", "marginRight": "20px", "verticalAlign": "middle"}
            ),
            html.Span("NEM Price Monitor", style={
                "fontSize": "24px",
                "fontWeight": "500",
                "verticalAlign": "middle",
            }),
            html.Span(
                _source_label,
                style={
                    "backgroundColor": _source_color,
                    "color": "white",
                    "padding": "4px 12px",
                    "borderRadius": "4px",
                    "fontSize": "12px",
                    "fontWeight": "600",
                    "marginLeft": "15px",
                    "verticalAlign": "middle",
                    "animation": "pulse 2s infinite" if _source_label == "LIVE" else "none",
                }
            ) if _source_label else None,
        ], style={"display": "flex", "alignItems": "center"}),
        html.P(
            f"Real-time spot prices across the Australian National Electricity Market • {_source_desc}",
            style={"margin": "10px 0 0 0", "opacity": 0.7, "fontSize": "13px"}
        ),
    ], style={
        "backgroundColor": AGL_DARK,
        "color": "white",
        "padding": "20px 30px",
        "marginBottom": "0",
        "borderBottom": f"4px solid {AGL_ORANGE}",
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

    # Convert to AEDT for display
    last_time_aedt = to_aedt(last_time)
    if last_time_aedt:
        time_str = last_time_aedt.strftime('%d %b %Y %H:%M:%S AEDT')
    else:
        time_str = last_time.strftime('%Y-%m-%d %H:%M:%S')

    return cards, f"Last updated: {time_str}"


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
        labels={"rrp": "Price ($/MWh)", "timestamp": "Time (AEDT)", "region": "Region"},
    )

    # Add threshold lines
    fig.add_hline(y=PRICE_WARNING, line_dash="dash", line_color=AGL_ORANGE, opacity=0.5,
                  annotation_text="Warning ($100)")
    fig.add_hline(y=PRICE_ALERT, line_dash="dash", line_color="#E4002B", opacity=0.5,
                  annotation_text="Alert ($300)")

    # AGL-themed layout
    fig.update_layout(
        margin=dict(l=50, r=20, t=30, b=50),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=11),
        ),
        hovermode="x unified",
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Montserrat, sans-serif", color=AGL_DARK),
        xaxis=dict(
            showgrid=True,
            gridcolor="#E0E0E0",
            linecolor="#E0E0E0",
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="#E0E0E0",
            linecolor="#E0E0E0",
        ),
    )

    # Thicker lines for better visibility
    fig.update_traces(line=dict(width=2.5))

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
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Montserrat, sans-serif", color=AGL_DARK),
        xaxis=dict(showgrid=False, linecolor="#E0E0E0"),
        yaxis=dict(showgrid=True, gridcolor="#E0E0E0", linecolor="#E0E0E0"),
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
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Montserrat, sans-serif", color=AGL_DARK),
        xaxis=dict(showgrid=False, linecolor="#E0E0E0"),
        yaxis=dict(showgrid=True, gridcolor="#E0E0E0", linecolor="#E0E0E0"),
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
