"""
NEM Forecast Dashboard - Databricks App

Dual-mode forecast dashboard for Australian electricity market:
- Tab 0: Lab Overview (story, architecture, run flow)
- Tab 1: Load Forecast (actual vs predicted TOTALDEMAND)
- Tab 2: Price Forecast (actual vs predicted RRP)

Reads predictions from Delta tables written by the ML workshop notebooks.
Supports sample data mode for local development when Delta is unavailable.

Deploy to Databricks:
    databricks apps create nem-forecast
    databricks apps deploy nem-forecast --source-code-path /Workspace/Users/.../app
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

# Databricks SQL connector (available in Databricks runtime)
try:
    from databricks import sql as databricks_sql
    DATABRICKS_AVAILABLE = True
except ImportError:
    DATABRICKS_AVAILABLE = False

try:
    from databricks.sdk.core import Config as DatabricksConfig
    DATABRICKS_SDK_AVAILABLE = True
except ImportError:
    DATABRICKS_SDK_AVAILABLE = False

# Configuration from environment variables
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
_host = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_HOST = _host.replace("https://", "").replace("http://", "").rstrip("/")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
CATALOG = os.environ.get("NEMWEB_CATALOG") or os.environ.get("DATABRICKS_CATALOG", "daveok")
SCHEMA = os.environ.get("NEMWEB_SCHEMA") or os.environ.get("DATABRICKS_SCHEMA", "ml_workshops")

# NEM Regions
NEM_REGIONS = ["NSW1", "VIC1", "QLD1", "SA1", "TAS1"]

# Region display config
REGION_CONFIG = {
    "NSW1": {"name": "New South Wales", "color": "#FF6900"},
    "VIC1": {"name": "Victoria", "color": "#00A9E0"},
    "QLD1": {"name": "Queensland", "color": "#7AB800"},
    "SA1":  {"name": "South Australia", "color": "#E4002B"},
    "TAS1": {"name": "Tasmania", "color": "#6D2077"},
}

# AGL-inspired theme colors
BRAND_PRIMARY = "#002B5C"
BRAND_ACCENT = "#00B5E2"
BRAND_DARK = "#0F172A"
BRAND_LIGHT = "#F4F8FC"
BRAND_SURFACE = "#FFFFFF"
BRAND_BORDER = "#DCE6F2"
BRAND_MUTED = "#64748B"


def _format_mmss(total_seconds: int) -> str:
    """Format seconds as MM:SS."""
    total_seconds = max(0, int(total_seconds))
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes:02d}:{seconds:02d}"


def _latest_timestamp_iso(*dfs: pd.DataFrame) -> Optional[str]:
    """Return latest timestamp across known timestamp columns."""
    latest_ts = None
    for df in dfs:
        if df is None or df.empty:
            continue
        for col in ("scored_at", "settlement_date", "hour"):
            if col in df.columns:
                ts = pd.to_datetime(df[col], errors="coerce").max()
                if pd.notna(ts) and (latest_ts is None or ts > latest_ts):
                    latest_ts = ts
                break
    return latest_ts.isoformat() if latest_ts is not None else None


def _sql_query(query: str) -> Optional[pd.DataFrame]:
    """Execute SQL query against Databricks warehouse."""
    if not DATABRICKS_AVAILABLE or not WAREHOUSE_ID:
        return None

    connect_kwargs = {
        "http_path": f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
    }

    if DATABRICKS_HOST and DATABRICKS_TOKEN:
        connect_kwargs["server_hostname"] = DATABRICKS_HOST
        connect_kwargs["access_token"] = DATABRICKS_TOKEN
    elif DATABRICKS_SDK_AVAILABLE:
        try:
            config = DatabricksConfig()
            sdk_host = config.host.replace("https://", "").replace("http://", "").rstrip("/")
            connect_kwargs["server_hostname"] = sdk_host
            connect_kwargs["credentials_provider"] = lambda: config.authenticate
        except Exception as e:
            print(f"SDK config unavailable for SQL connection: {e}")
            return None
    else:
        return None

    try:
        with databricks_sql.connect(**connect_kwargs) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        print(f"SQL query failed: {e}")
        return None


def fetch_demand_predictions() -> tuple[pd.DataFrame, str]:
    """Fetch demand predictions from Delta table.

    Falls back to AEMO forecast-vs-actual data from streaming tables when
    workshop prediction tables are not yet available.
    """
    df = _sql_query(f"""
        SELECT settlement_date, region_id,
               actual_demand_mw, predicted_demand_mw, forecast_error_mw,
               model_version, scored_at
        FROM {CATALOG}.{SCHEMA}.demand_predictions
        ORDER BY settlement_date DESC
        LIMIT 5000
    """)
    if df is not None and not df.empty:
        df["settlement_date"] = pd.to_datetime(df["settlement_date"])
        return df, "MODEL"

    # Fallback: use AEMO forecast tables produced by the streaming pipeline.
    fallback_df = _sql_query(f"""
        SELECT settlement_date, region_id,
               actual_demand_mw,
               forecast_demand_mw AS predicted_demand_mw,
               demand_error_mw AS forecast_error_mw,
               0 AS model_version,
               forecast_run_time AS scored_at
        FROM {CATALOG}.{SCHEMA}.silver_forecast_vs_actual
        ORDER BY settlement_date DESC
        LIMIT 5000
    """)
    if fallback_df is not None and not fallback_df.empty:
        fallback_df["settlement_date"] = pd.to_datetime(fallback_df["settlement_date"])
        return fallback_df, "AEMO_FALLBACK"

    return generate_sample_demand_predictions(), "SAMPLE"


def fetch_price_predictions() -> tuple[pd.DataFrame, str]:
    """Fetch price predictions from Delta table.

    Falls back to AEMO forecast-vs-actual data from streaming tables when
    workshop prediction tables are not yet available.
    """
    df = _sql_query(f"""
        SELECT settlement_date, region_id,
               actual_rrp, predicted_rrp, forecast_error_dollars,
               model_version, scored_at
        FROM {CATALOG}.{SCHEMA}.price_predictions
        ORDER BY settlement_date DESC
        LIMIT 5000
    """)
    if df is not None and not df.empty:
        df["settlement_date"] = pd.to_datetime(df["settlement_date"])
        return df, "MODEL"

    # Fallback: use AEMO forecast tables produced by the streaming pipeline.
    fallback_df = _sql_query(f"""
        SELECT settlement_date, region_id,
               actual_rrp,
               forecast_rrp AS predicted_rrp,
               price_error AS forecast_error_dollars,
               0 AS model_version,
               forecast_run_time AS scored_at
        FROM {CATALOG}.{SCHEMA}.silver_forecast_vs_actual
        ORDER BY settlement_date DESC
        LIMIT 5000
    """)
    if fallback_df is not None and not fallback_df.empty:
        fallback_df["settlement_date"] = pd.to_datetime(fallback_df["settlement_date"])
        return fallback_df, "AEMO_FALLBACK"

    return generate_sample_price_predictions(), "SAMPLE"


def fetch_gold_demand_hourly() -> pd.DataFrame:
    """Fetch hourly regional demand/weather summary from streaming gold."""
    df = _sql_query(f"""
        SELECT region_id, hour, avg_demand_mw, avg_rrp, air_temp_c, interval_count
        FROM {CATALOG}.{SCHEMA}.gold_demand_hourly
        ORDER BY hour DESC
        LIMIT 2000
    """)
    if df is not None and not df.empty:
        df["hour"] = pd.to_datetime(df["hour"])
        return df
    return pd.DataFrame()


def fetch_gold_price_hourly() -> pd.DataFrame:
    """Fetch hourly regional price summary from streaming gold."""
    df = _sql_query(f"""
        SELECT region_id, hour, avg_rrp, min_rrp, max_rrp, stddev_rrp, interval_count
        FROM {CATALOG}.{SCHEMA}.gold_price_hourly
        ORDER BY hour DESC
        LIMIT 2000
    """)
    if df is not None and not df.empty:
        df["hour"] = pd.to_datetime(df["hour"])
        return df
    return pd.DataFrame()


def fetch_gold_forecast_accuracy() -> pd.DataFrame:
    """Fetch hourly AEMO forecast accuracy metrics from streaming gold."""
    df = _sql_query(f"""
        SELECT region_id, hour, demand_mae_mw, demand_bias_mw, demand_mape_pct,
               price_mae, price_bias, avg_lead_time_minutes, interval_count
        FROM {CATALOG}.{SCHEMA}.gold_forecast_accuracy
        ORDER BY hour DESC
        LIMIT 2000
    """)
    if df is not None and not df.empty:
        df["hour"] = pd.to_datetime(df["hour"])
        return df
    return pd.DataFrame()


def fetch_gold_interconnector_hourly() -> pd.DataFrame:
    """Fetch hourly interconnector utilisation metrics from streaming gold."""
    df = _sql_query(f"""
        SELECT INTERCONNECTORID, hour, avg_flow_mw, avg_utilisation_pct, interval_count
        FROM {CATALOG}.{SCHEMA}.gold_interconnector_hourly
        ORDER BY hour DESC
        LIMIT 2000
    """)
    if df is not None and not df.empty:
        df["hour"] = pd.to_datetime(df["hour"])
        return df
    return pd.DataFrame()


def generate_sample_demand_predictions() -> pd.DataFrame:
    """Generate sample demand prediction data for demo/local dev."""
    timestamps = pd.date_range(
        start=datetime.now() - timedelta(hours=24),
        end=datetime.now(),
        freq="5min",
    )
    rows = []
    for ts in timestamps:
        hour = ts.hour
        base = 7500 + 2000 * np.sin((hour - 6) * np.pi / 12)
        actual = base + np.random.normal(0, 150)
        predicted = actual + np.random.normal(0, 60)
        rows.append({
            "settlement_date": ts,
            "region_id": "NSW1",
            "actual_demand_mw": round(actual, 1),
            "predicted_demand_mw": round(predicted, 1),
            "forecast_error_mw": round(predicted - actual, 1),
            "model_version": 1,
            "scored_at": datetime.now(),
        })
    return pd.DataFrame(rows)


def generate_sample_price_predictions() -> pd.DataFrame:
    """Generate sample price prediction data for demo/local dev."""
    timestamps = pd.date_range(
        start=datetime.now() - timedelta(hours=24),
        end=datetime.now(),
        freq="5min",
    )
    rows = []
    for ts in timestamps:
        hour = ts.hour
        base = 50 + 30 * np.sin((hour - 6) * np.pi / 12)
        actual = max(0, base + np.random.normal(0, 15))
        predicted = max(0, actual + np.random.normal(0, 8))
        rows.append({
            "settlement_date": ts,
            "region_id": "NSW1",
            "actual_rrp": round(actual, 2),
            "predicted_rrp": round(predicted, 2),
            "forecast_error_dollars": round(predicted - actual, 2),
            "model_version": 1,
            "scored_at": datetime.now(),
        })
    return pd.DataFrame(rows)


# Initialize Dash app
app = dash.Dash(
    __name__,
    title="NEM Forecast Dashboard",
    update_title="Updating...",
    suppress_callback_exceptions=True,
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap"
    ],
)

app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            * { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; }
            body { margin: 0; background-color: #F4F8FC; }
            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.7; }
            }
            .tab-active {
                border-bottom: 3px solid #00B5E2 !important;
                font-weight: 600 !important;
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

server = app.server

# Determine data source mode
_has_delta = bool(
    WAREHOUSE_ID
    and DATABRICKS_AVAILABLE
    and (
        (DATABRICKS_HOST and DATABRICKS_TOKEN)
        or DATABRICKS_SDK_AVAILABLE
    )
)
_source_label = "LIVE" if _has_delta else "DEMO"
_source_color = BRAND_ACCENT if _has_delta else "#6c757d"

# App layout
app.layout = html.Div([
    # Header
    html.Div([
        html.Div([
            html.Span("NEM Forecast Dashboard", style={
                "fontSize": "24px", "fontWeight": "600",
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
                    "animation": "pulse 2s infinite" if _has_delta else "none",
                },
            ),
        ], style={"display": "flex", "alignItems": "center"}),
        html.P(
            "ML model predictions vs actuals for load and price forecasting",
            style={"margin": "8px 0 0 0", "opacity": 0.7, "fontSize": "13px"},
        ),
    ], style={
        "background": f"linear-gradient(120deg, {BRAND_PRIMARY} 0%, #0066CC 100%)",
        "color": "white",
        "padding": "22px 30px",
        "borderBottom": f"4px solid {BRAND_ACCENT}",
        "boxShadow": "0 2px 8px rgba(15, 23, 42, 0.25)",
    }),

    # Tab selector
    dcc.Tabs(
        id="forecast-tabs",
        value="overview",
        children=[
            dcc.Tab(label="Lab Overview", value="overview", style={
                "padding": "12px 24px", "fontSize": "14px",
                "backgroundColor": BRAND_LIGHT, "border": "none",
                "color": BRAND_MUTED,
            }, selected_style={
                "padding": "12px 24px", "fontSize": "14px",
                "backgroundColor": BRAND_SURFACE, "color": BRAND_PRIMARY,
                "borderTop": f"3px solid {BRAND_ACCENT}", "fontWeight": "600",
            }),
            dcc.Tab(label="Load Forecast", value="demand", style={
                "padding": "12px 24px", "fontSize": "14px",
                "backgroundColor": BRAND_LIGHT, "border": "none",
                "color": BRAND_MUTED,
            }, selected_style={
                "padding": "12px 24px", "fontSize": "14px",
                "backgroundColor": BRAND_SURFACE, "color": BRAND_PRIMARY,
                "borderTop": f"3px solid {BRAND_ACCENT}", "fontWeight": "600",
            }),
            dcc.Tab(label="Price Forecast", value="price", style={
                "padding": "12px 24px", "fontSize": "14px",
                "backgroundColor": BRAND_LIGHT, "border": "none",
                "color": BRAND_MUTED,
            }, selected_style={
                "padding": "12px 24px", "fontSize": "14px",
                "backgroundColor": BRAND_SURFACE, "color": BRAND_PRIMARY,
                "borderTop": f"3px solid {BRAND_ACCENT}", "fontWeight": "600",
            }),
            dcc.Tab(label="Pipeline Overview", value="pipeline", style={
                "padding": "12px 24px", "fontSize": "14px",
                "backgroundColor": BRAND_LIGHT, "border": "none",
                "color": BRAND_MUTED,
            }, selected_style={
                "padding": "12px 24px", "fontSize": "14px",
                "backgroundColor": BRAND_SURFACE, "color": BRAND_PRIMARY,
                "borderTop": f"3px solid {BRAND_ACCENT}", "fontWeight": "600",
            }),
        ],
        style={
            "padding": "0 20px",
            "marginTop": "10px",
            "backgroundColor": BRAND_SURFACE,
            "borderBottom": f"1px solid {BRAND_BORDER}",
        },
    ),

    # Prediction cadence strip
    html.Div([
        html.Div([
            html.P("Next App Refresh", style={"margin": "0", "fontSize": "12px", "color": BRAND_MUTED}),
            html.P(
                id="countdown-next-refresh",
                children="01:00",
                style={"margin": "4px 0 0 0", "fontSize": "24px", "fontWeight": "700", "color": BRAND_PRIMARY},
            ),
        ], style={
            "flex": 1,
            "backgroundColor": BRAND_SURFACE,
            "border": f"1px solid {BRAND_BORDER}",
            "borderRadius": "10px",
            "padding": "12px 14px",
            "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
        }),
        html.Div([
            html.P("Next 5-Min NEM Interval", style={"margin": "0", "fontSize": "12px", "color": BRAND_MUTED}),
            html.P(
                id="countdown-next-settlement",
                children="05:00",
                style={"margin": "4px 0 0 0", "fontSize": "24px", "fontWeight": "700", "color": BRAND_PRIMARY},
            ),
        ], style={
            "flex": 1,
            "backgroundColor": BRAND_SURFACE,
            "border": f"1px solid {BRAND_BORDER}",
            "borderRadius": "10px",
            "padding": "12px 14px",
            "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
        }),
        html.Div([
            html.P("Latest Prediction Timestamp", style={"margin": "0", "fontSize": "12px", "color": BRAND_MUTED}),
            html.P(
                id="latest-score-label",
                children="Waiting for first refresh",
                style={"margin": "7px 0 0 0", "fontSize": "15px", "fontWeight": "600", "color": BRAND_PRIMARY},
            ),
        ], style={
            "flex": 1.4,
            "backgroundColor": BRAND_SURFACE,
            "border": f"1px solid {BRAND_BORDER}",
            "borderRadius": "10px",
            "padding": "12px 14px",
            "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
        }),
    ], style={"display": "flex", "gap": "12px", "padding": "14px 20px 6px 20px"}),
    html.Div([
        html.Div(id="interval-phase-label", children="Counting down to next 5-min interval", style={
            "fontSize": "12px",
            "color": BRAND_MUTED,
            "marginBottom": "6px",
        }),
        html.Div([
            html.Div(id="interval-progress-fill", style={
                "width": "0%",
                "height": "100%",
                "backgroundColor": BRAND_ACCENT,
                "borderRadius": "999px",
                "transition": "width 1s linear",
            }),
        ], style={
            "height": "10px",
            "backgroundColor": "#E2E8F0",
            "borderRadius": "999px",
            "overflow": "hidden",
        }),
    ], style={"padding": "0 20px 8px 20px"}),
    html.Div([
        html.Span("Data Source:", style={"fontSize": "12px", "color": BRAND_MUTED, "marginRight": "8px"}),
        html.Span(
            id="active-data-source-badge",
            children="OVERVIEW",
            style={
                "display": "inline-block",
                "padding": "4px 10px",
                "borderRadius": "999px",
                "backgroundColor": "#E2E8F0",
                "color": BRAND_DARK,
                "fontSize": "12px",
                "fontWeight": "700",
            },
        ),
    ], style={"padding": "0 20px 10px 20px"}),

    # Landing page section
    html.Div([
        html.Div([
            html.H2(
                "From NEMWEB streaming to live forecasting in one platform",
                style={"margin": "0 0 8px 0", "color": BRAND_PRIMARY, "fontWeight": "700"},
            ),
            html.P(
                "This demo shows the full Databricks workflow we built: ingest, feature "
                "engineering, ML training, model governance, prediction pipelines, and "
                "an interactive app for operators.",
                style={"margin": 0, "fontSize": "15px", "color": BRAND_DARK, "lineHeight": "1.6"},
            ),
        ], style={
            "backgroundColor": BRAND_SURFACE,
            "borderRadius": "10px",
            "padding": "20px",
            "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
            "border": f"1px solid {BRAND_BORDER}",
            "marginBottom": "16px",
        }),
        html.Div([
            html.Div([
                html.H4("1) Data + Pipelines", style={"margin": "0 0 8px 0", "color": BRAND_PRIMARY}),
                html.P(
                    "NEMWEB + weather feeds land in streaming bronze/silver/gold tables "
                    "with governed quality and freshness.",
                    style={"margin": 0, "fontSize": "13px", "lineHeight": "1.5"},
                ),
            ], style={
                "flex": 1, "backgroundColor": BRAND_SURFACE, "borderRadius": "10px",
                "padding": "14px", "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
                "border": f"1px solid {BRAND_BORDER}",
            }),
            html.Div([
                html.H4("2) Model Factory", style={"margin": "0 0 8px 0", "color": BRAND_PRIMARY}),
                html.P(
                    "Load and price workshops train models, track experiments in MLflow, "
                    "and register champion aliases in Unity Catalog.",
                    style={"margin": 0, "fontSize": "13px", "lineHeight": "1.5"},
                ),
            ], style={
                "flex": 1, "backgroundColor": BRAND_SURFACE, "borderRadius": "10px",
                "padding": "14px", "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
                "border": f"1px solid {BRAND_BORDER}",
            }),
            html.Div([
                html.H4("3) Serving + Decisions", style={"margin": "0 0 8px 0", "color": BRAND_PRIMARY}),
                html.P(
                    "Prediction pipeline refreshes app-facing tables that power operator "
                    "views for forecast quality and market behavior.",
                    style={"margin": 0, "fontSize": "13px", "lineHeight": "1.5"},
                ),
            ], style={
                "flex": 1, "backgroundColor": BRAND_SURFACE, "borderRadius": "10px",
                "padding": "14px", "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
                "border": f"1px solid {BRAND_BORDER}",
            }),
        ], style={"display": "flex", "gap": "14px", "marginBottom": "16px"}),
        html.Div([
            html.H4("Recommended lab flow", style={"margin": "0 0 8px 0", "color": BRAND_PRIMARY}),
            html.P(
                "Start with this overview, then go to Load Forecast, Price Forecast, and "
                "Pipeline Overview tabs in sequence.",
                style={"margin": "0 0 8px 0", "fontSize": "13px", "lineHeight": "1.5"},
            ),
            html.Div([
                html.Span(
                    "Streaming Pipeline",
                    style={
                        "padding": "6px 10px", "borderRadius": "999px",
                        "backgroundColor": "#E6F7FF", "color": BRAND_PRIMARY, "fontWeight": "600",
                    },
                ),
                html.Span("->", style={"color": BRAND_MUTED, "fontWeight": "600"}),
                html.Span(
                    "Load + Price Workshops",
                    style={
                        "padding": "6px 10px", "borderRadius": "999px",
                        "backgroundColor": "#ECFDF3", "color": "#166534", "fontWeight": "600",
                    },
                ),
                html.Span("->", style={"color": BRAND_MUTED, "fontWeight": "600"}),
                html.Span(
                    "Prediction Pipeline",
                    style={
                        "padding": "6px 10px", "borderRadius": "999px",
                        "backgroundColor": "#FFF7ED", "color": "#9A3412", "fontWeight": "600",
                    },
                ),
                html.Span("->", style={"color": BRAND_MUTED, "fontWeight": "600"}),
                html.Span(
                    "Live App",
                    style={
                        "padding": "6px 10px", "borderRadius": "999px",
                        "backgroundColor": "#EEF2FF", "color": "#3730A3", "fontWeight": "600",
                    },
                ),
            ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap"}),
        ], style={
            "backgroundColor": BRAND_SURFACE,
            "borderRadius": "10px",
            "padding": "16px",
            "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
            "border": f"1px solid {BRAND_BORDER}",
        }),
    ], id="landing-section", style={"padding": "20px 20px 8px 20px"}),

    # KPI cards
    html.Div([
        html.Div(id="kpi-cards", style={
            "display": "flex", "flexWrap": "wrap", "gap": "15px",
            "padding": "20px 20px 10px 20px",
        }),
    ], id="kpi-section"),

    # Main chart
    html.Div([
        dcc.Graph(id="forecast-chart", style={"height": "400px"}),
    ], id="forecast-chart-section", style={
        "padding": "0 20px",
        "marginBottom": "10px",
        "backgroundColor": BRAND_SURFACE,
        "borderRadius": "10px",
        "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
        "marginLeft": "20px",
        "marginRight": "20px",
    }),

    # Error distribution + feature importance side by side
    html.Div([
        html.Div([
            html.H4("Forecast Error Distribution", style={"marginBottom": "5px"}),
            dcc.Graph(id="error-chart", style={"height": "300px"}),
        ], style={
            "flex": 1,
            "backgroundColor": BRAND_SURFACE,
            "borderRadius": "10px",
            "padding": "10px",
            "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
        }),
        html.Div([
            html.H4("Model Metrics", style={"marginBottom": "5px"}),
            html.Div(id="metrics-panel"),
        ], style={
            "flex": 1,
            "backgroundColor": BRAND_SURFACE,
            "borderRadius": "10px",
            "padding": "10px",
            "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
        }),
    ], id="detail-section", style={"display": "flex", "gap": "20px", "padding": "0 20px", "marginBottom": "10px"}),

    # Data table
    html.Div([
        html.H4("Recent Predictions", style={"marginBottom": "5px"}),
        dag.AgGrid(
            id="predictions-table",
            columnDefs=[],
            defaultColDef={"resizable": True, "minWidth": 100, "sortable": True},
            dashGridOptions={"pagination": True, "paginationPageSize": 15},
            style={"height": "400px"},
        ),
    ], id="table-section", style={
        "padding": "10px 20px 20px 20px",
        "marginBottom": "20px",
        "backgroundColor": BRAND_SURFACE,
        "borderRadius": "10px",
        "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
        "marginLeft": "20px",
        "marginRight": "20px",
    }),

    # Auto-refresh
    dcc.Interval(id="refresh-interval", interval=60 * 1000, n_intervals=0),
    dcc.Interval(id="clock-interval", interval=1000, n_intervals=0),

    # Data stores
    dcc.Store(id="demand-data"),
    dcc.Store(id="price-data"),
    dcc.Store(id="gold-demand-data"),
    dcc.Store(id="gold-price-data"),
    dcc.Store(id="gold-accuracy-data"),
    dcc.Store(id="gold-interconnector-data"),
    dcc.Store(id="demand-source"),
    dcc.Store(id="price-source"),
    dcc.Store(id="last-refresh-ts"),
    dcc.Store(id="latest-score-ts"),

    # Footer
    html.Div([
        html.P([
            "Data: ",
            html.A("AEMO NEMWEB", href="https://www.nemweb.com.au", target="_blank"),
            " + ",
            html.A("BOM Weather", href="http://www.bom.gov.au", target="_blank"),
            " | Powered by Databricks + MLflow | ",
            html.Span(id="last-updated"),
        ], style={"margin": 0, "opacity": 0.7}),
    ], style={
        "backgroundColor": "#EDF4FB",
        "padding": "15px 20px",
        "textAlign": "center",
        "fontSize": "13px",
    }),
], style={"backgroundColor": BRAND_LIGHT, "minHeight": "100vh"})


@callback(
    Output("demand-data", "data"),
    Output("price-data", "data"),
    Output("gold-demand-data", "data"),
    Output("gold-price-data", "data"),
    Output("gold-accuracy-data", "data"),
    Output("gold-interconnector-data", "data"),
    Output("demand-source", "data"),
    Output("price-source", "data"),
    Output("last-refresh-ts", "data"),
    Output("latest-score-ts", "data"),
    Input("refresh-interval", "n_intervals"),
)
def refresh_data(n):
    """Fetch fresh prediction and pipeline gold data."""
    demand_df, demand_source = fetch_demand_predictions()
    price_df, price_source = fetch_price_predictions()
    gold_demand_df = fetch_gold_demand_hourly()
    gold_price_df = fetch_gold_price_hourly()
    gold_accuracy_df = fetch_gold_forecast_accuracy()
    gold_interconnector_df = fetch_gold_interconnector_hourly()
    refreshed_at = datetime.now().isoformat(timespec="seconds")
    latest_score_ts = _latest_timestamp_iso(demand_df, price_df)
    return (
        demand_df.to_json(date_format="iso", orient="split"),
        price_df.to_json(date_format="iso", orient="split"),
        gold_demand_df.to_json(date_format="iso", orient="split"),
        gold_price_df.to_json(date_format="iso", orient="split"),
        gold_accuracy_df.to_json(date_format="iso", orient="split"),
        gold_interconnector_df.to_json(date_format="iso", orient="split"),
        demand_source,
        price_source,
        refreshed_at,
        latest_score_ts,
    )


@callback(
    Output("countdown-next-refresh", "children"),
    Output("countdown-next-settlement", "children"),
    Output("latest-score-label", "children"),
    Output("interval-progress-fill", "style"),
    Output("interval-phase-label", "children"),
    Input("clock-interval", "n_intervals"),
    Input("last-refresh-ts", "data"),
    Input("latest-score-ts", "data"),
)
def update_prediction_timer(_tick, last_refresh_ts, latest_score_ts):
    """Update prediction and interval countdown timers."""
    now = datetime.now()

    refresh_remaining = 60
    if last_refresh_ts:
        try:
            last_refresh = datetime.fromisoformat(last_refresh_ts)
            elapsed = (now - last_refresh).total_seconds()
            refresh_remaining = max(0, 60 - int(elapsed))
        except Exception:
            refresh_remaining = 60

    elapsed_in_window = (now.minute % 5) * 60 + now.second
    settlement_remaining = 300 - elapsed_in_window
    if settlement_remaining == 300:
        settlement_remaining = 0
    progress_pct = max(0.0, min(100.0, (elapsed_in_window / 300.0) * 100.0))

    if latest_score_ts:
        try:
            latest_dt = datetime.fromisoformat(latest_score_ts)
            latest_score_label = latest_dt.strftime("%d %b %H:%M:%S")
        except Exception:
            latest_score_label = "Unknown"
    else:
        latest_score_label = "Waiting for first refresh"

    phase_label = (
        "New interval window opened"
        if settlement_remaining <= 5
        else "Counting down to next 5-min interval"
    )
    progress_style = {
        "width": f"{progress_pct:.1f}%",
        "height": "100%",
        "backgroundColor": BRAND_ACCENT,
        "borderRadius": "999px",
        "transition": "width 1s linear",
    }

    return (
        _format_mmss(refresh_remaining),
        _format_mmss(settlement_remaining),
        latest_score_label,
        progress_style,
        phase_label,
    )


@callback(
    Output("active-data-source-badge", "children"),
    Output("active-data-source-badge", "style"),
    Input("forecast-tabs", "value"),
    Input("demand-source", "data"),
    Input("price-source", "data"),
)
def update_data_source_badge(tab, demand_source, price_source):
    """Show explicit source provenance for current tab."""
    source_styles = {
        "MODEL": {"backgroundColor": "#DCFCE7", "color": "#166534"},
        "AEMO_FALLBACK": {"backgroundColor": "#FEF3C7", "color": "#92400E"},
        "SAMPLE": {"backgroundColor": "#FEE2E2", "color": "#991B1B"},
        "PIPELINE": {"backgroundColor": "#DBEAFE", "color": "#1E40AF"},
        "OVERVIEW": {"backgroundColor": "#E2E8F0", "color": BRAND_DARK},
    }

    if tab == "demand":
        source = demand_source or "OVERVIEW"
        label = f"Demand: {source}"
    elif tab == "price":
        source = price_source or "OVERVIEW"
        label = f"Price: {source}"
    elif tab == "pipeline":
        source = "PIPELINE"
        label = "Pipeline: STREAMING_GOLD"
    else:
        source = "OVERVIEW"
        label = "Overview"

    style = {
        "display": "inline-block",
        "padding": "4px 10px",
        "borderRadius": "999px",
        "fontSize": "12px",
        "fontWeight": "700",
    }
    style.update(source_styles[source])
    return label, style


@callback(
    Output("landing-section", "style"),
    Output("kpi-section", "style"),
    Output("forecast-chart-section", "style"),
    Output("detail-section", "style"),
    Output("table-section", "style"),
    Input("forecast-tabs", "value"),
)
def toggle_sections_for_tab(tab):
    """Show landing page on overview tab, analytics on data tabs."""
    if tab == "overview":
        return (
            {"padding": "20px 20px 8px 20px", "display": "block"},
            {"display": "none"},
            {"display": "none"},
            {"display": "none"},
            {"display": "none"},
        )
    return (
        {"display": "none"},
        {"display": "block"},
        {
            "padding": "0 20px",
            "marginBottom": "10px",
            "backgroundColor": BRAND_SURFACE,
            "borderRadius": "10px",
            "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
            "marginLeft": "20px",
            "marginRight": "20px",
            "display": "block",
        },
        {"display": "flex", "gap": "20px", "padding": "0 20px", "marginBottom": "10px"},
        {
            "padding": "10px 20px 20px 20px",
            "marginBottom": "20px",
            "backgroundColor": BRAND_SURFACE,
            "borderRadius": "10px",
            "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
            "marginLeft": "20px",
            "marginRight": "20px",
            "display": "block",
        },
    )


@callback(
    Output("kpi-cards", "children"),
    Output("last-updated", "children"),
    Input("forecast-tabs", "value"),
    Input("demand-data", "data"),
    Input("price-data", "data"),
    Input("gold-demand-data", "data"),
    Input("gold-price-data", "data"),
    Input("gold-accuracy-data", "data"),
    Input("gold-interconnector-data", "data"),
)
def update_kpi_cards(
    tab, demand_json, price_json, gold_demand_json, gold_price_json, gold_accuracy_json, gold_interconnector_json
):
    """Update KPI cards based on selected tab."""
    if tab == "demand" and demand_json:
        df = pd.read_json(demand_json, orient="split")
        if df.empty:
            return [], "No data"
        latest = df.sort_values("settlement_date").iloc[-1]
        actual = latest["actual_demand_mw"]
        predicted = latest["predicted_demand_mw"]
        error = abs(latest["forecast_error_mw"])
        mae = df["forecast_error_mw"].abs().mean()
        mape = (df["forecast_error_mw"].abs() / df["actual_demand_mw"].abs().clip(lower=0.01)).mean() * 100

        cards = [
            _kpi_card("Actual Demand", f"{actual:,.0f} MW", BRAND_PRIMARY),
            _kpi_card("Predicted Demand", f"{predicted:,.0f} MW", BRAND_ACCENT),
            _kpi_card("Current Error", f"{error:,.0f} MW", "#28a745" if error < 100 else "#dc3545"),
            _kpi_card("MAE", f"{mae:,.0f} MW", BRAND_PRIMARY),
            _kpi_card("MAPE", f"{mape:.2f}%", BRAND_PRIMARY),
        ]
    elif tab == "price" and price_json:
        df = pd.read_json(price_json, orient="split")
        if df.empty:
            return [], "No data"
        latest = df.sort_values("settlement_date").iloc[-1]
        actual = latest["actual_rrp"]
        predicted = latest["predicted_rrp"]
        error = abs(latest["forecast_error_dollars"])
        mae = df["forecast_error_dollars"].abs().mean()
        mape = (df["forecast_error_dollars"].abs() / df["actual_rrp"].abs().clip(lower=1)).mean() * 100

        cards = [
            _kpi_card("Actual Price", f"${actual:,.2f}/MWh", BRAND_PRIMARY),
            _kpi_card("Predicted Price", f"${predicted:,.2f}/MWh", BRAND_ACCENT),
            _kpi_card("Current Error", f"${error:,.2f}", "#28a745" if error < 10 else "#dc3545"),
            _kpi_card("MAE", f"${mae:,.2f}/MWh", BRAND_PRIMARY),
            _kpi_card("MAPE", f"{mape:.1f}%", BRAND_PRIMARY),
        ]
    elif tab == "pipeline" and gold_demand_json and gold_price_json and gold_accuracy_json:
        demand_df = pd.read_json(gold_demand_json, orient="split")
        price_df = pd.read_json(gold_price_json, orient="split")
        acc_df = pd.read_json(gold_accuracy_json, orient="split")
        inter_df = (
            pd.read_json(gold_interconnector_json, orient="split")
            if gold_interconnector_json else pd.DataFrame()
        )

        if demand_df.empty or price_df.empty or acc_df.empty:
            return [], "No data"

        latest_demand = demand_df.sort_values("hour").iloc[-1]
        latest_price = price_df.sort_values("hour").iloc[-1]
        latest_acc = acc_df.sort_values("hour").iloc[-1]

        util_label = "N/A"
        if not inter_df.empty and "avg_utilisation_pct" in inter_df.columns:
            util_label = f"{inter_df['avg_utilisation_pct'].mean():.1f}%"

        cards = [
            _kpi_card("Hourly Demand", f"{latest_demand['avg_demand_mw']:,.0f} MW", BRAND_PRIMARY),
            _kpi_card("Hourly Price", f"${latest_price['avg_rrp']:,.2f}/MWh", BRAND_ACCENT),
            _kpi_card("AEMO Demand MAPE", f"{latest_acc['demand_mape_pct']:.2f}%", BRAND_PRIMARY),
            _kpi_card("AEMO Price MAE", f"${latest_acc['price_mae']:.2f}/MWh", BRAND_PRIMARY),
            _kpi_card("Interconnector Util.", util_label, BRAND_PRIMARY),
        ]
    elif tab == "overview":
        now_str = datetime.now().strftime("%d %b %Y %H:%M:%S")
        return [], f"Last updated: {now_str}"
    else:
        return [], "No data"

    now_str = datetime.now().strftime("%d %b %Y %H:%M:%S")
    return cards, f"Last updated: {now_str}"


def _kpi_card(title: str, value: str, color: str) -> html.Div:
    """Create a KPI card component."""
    return html.Div([
        html.P(title, style={
            "margin": "0 0 4px 0", "fontSize": "12px",
            "fontWeight": "500", "opacity": 0.8, "textTransform": "uppercase",
            "color": BRAND_MUTED,
        }),
        html.P(value, style={
            "margin": 0, "fontSize": "28px", "fontWeight": "700", "color": color,
        }),
    ], style={
        "backgroundColor": BRAND_SURFACE,
        "borderRadius": "8px",
        "padding": "16px 20px",
        "minWidth": "140px",
        "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
        "borderTop": f"3px solid {color}",
        "border": f"1px solid {BRAND_BORDER}",
    })


def _empty_figure(title: str, message: str) -> go.Figure:
    """Create a readable empty-state chart."""
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        font=dict(size=14, color=BRAND_MUTED),
    )
    fig.update_layout(
        title=title,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        plot_bgcolor=BRAND_SURFACE,
        paper_bgcolor=BRAND_SURFACE,
        margin=dict(l=40, r=40, t=60, b=40),
    )
    return fig


@callback(
    Output("forecast-chart", "figure"),
    Input("forecast-tabs", "value"),
    Input("demand-data", "data"),
    Input("price-data", "data"),
    Input("gold-demand-data", "data"),
    Input("gold-price-data", "data"),
)
def update_forecast_chart(tab, demand_json, price_json, gold_demand_json, gold_price_json):
    """Update the main actual vs predicted chart."""
    if tab == "demand" and demand_json:
        df = pd.read_json(demand_json, orient="split")
        df["settlement_date"] = pd.to_datetime(df["settlement_date"])
        df = df.sort_values("settlement_date")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["settlement_date"], y=df["actual_demand_mw"],
            name="Actual Demand", line=dict(color=BRAND_PRIMARY, width=2),
        ))
        fig.add_trace(go.Scatter(
            x=df["settlement_date"], y=df["predicted_demand_mw"],
            name="Predicted Demand", line=dict(color=BRAND_ACCENT, width=2, dash="dot"),
        ))
        fig.update_layout(
            title="Actual vs Predicted Demand (MW)",
            yaxis_title="Demand (MW)",
        )

    elif tab == "price" and price_json:
        df = pd.read_json(price_json, orient="split")
        df["settlement_date"] = pd.to_datetime(df["settlement_date"])
        df = df.sort_values("settlement_date")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["settlement_date"], y=df["actual_rrp"],
            name="Actual RRP", line=dict(color=BRAND_PRIMARY, width=2),
        ))
        fig.add_trace(go.Scatter(
            x=df["settlement_date"], y=df["predicted_rrp"],
            name="Predicted RRP", line=dict(color=BRAND_ACCENT, width=2, dash="dot"),
        ))
        fig.update_layout(
            title="Actual vs Predicted Price ($/MWh)",
            yaxis_title="Price ($/MWh)",
        )
    elif tab == "pipeline" and gold_demand_json and gold_price_json:
        demand_df = pd.read_json(gold_demand_json, orient="split")
        price_df = pd.read_json(gold_price_json, orient="split")
        if demand_df.empty or price_df.empty:
            return _empty_figure(
                "Pipeline Gold: Hourly Demand and Price",
                "No pipeline data yet. Run the streaming pipeline refresh.",
            )

        demand_df = demand_df.sort_values("hour")
        price_df = price_df.sort_values("hour")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=demand_df["hour"], y=demand_df["avg_demand_mw"],
            name="Avg Demand (MW)", line=dict(color=BRAND_PRIMARY, width=2),
            yaxis="y",
        ))
        fig.add_trace(go.Scatter(
            x=price_df["hour"], y=price_df["avg_rrp"],
            name="Avg Price ($/MWh)", line=dict(color=BRAND_ACCENT, width=2),
            yaxis="y2",
        ))
        fig.update_layout(
            title="Pipeline Gold: Hourly Demand and Price",
            yaxis_title="Demand (MW)",
            yaxis2=dict(title="Price ($/MWh)", overlaying="y", side="right"),
        )
    else:
        fig = _empty_figure("Forecast", "Waiting for data...")

    fig.update_layout(
        xaxis_title="Time",
        margin=dict(l=60, r=20, t=50, b=50),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
        plot_bgcolor=BRAND_SURFACE,
        paper_bgcolor=BRAND_SURFACE,
        font=dict(family="Inter, sans-serif", color=BRAND_DARK),
        xaxis=dict(showgrid=True, gridcolor=BRAND_BORDER),
        yaxis=dict(showgrid=True, gridcolor=BRAND_BORDER),
    )
    return fig


@callback(
    Output("error-chart", "figure"),
    Input("forecast-tabs", "value"),
    Input("demand-data", "data"),
    Input("price-data", "data"),
    Input("gold-accuracy-data", "data"),
)
def update_error_chart(tab, demand_json, price_json, gold_accuracy_json):
    """Update the forecast error distribution chart."""
    if tab == "demand" and demand_json:
        df = pd.read_json(demand_json, orient="split")
        error_col = "forecast_error_mw"
        unit = "MW"
    elif tab == "price" and price_json:
        df = pd.read_json(price_json, orient="split")
        error_col = "forecast_error_dollars"
        unit = "$/MWh"
    elif tab == "pipeline" and gold_accuracy_json:
        df = pd.read_json(gold_accuracy_json, orient="split")
        if df.empty:
            return _empty_figure(
                "Forecast Error Distribution",
                "No accuracy data yet. Run the streaming pipeline refresh.",
            )

        latest = df["hour"].max()
        latest_df = df[df["hour"] == latest].sort_values("demand_mape_pct", ascending=False)
        fig = px.bar(
            latest_df,
            x="region_id",
            y="demand_mape_pct",
            labels={"region_id": "Region", "demand_mape_pct": "Demand MAPE (%)"},
            color="demand_mape_pct",
            color_continuous_scale="Oranges",
        )
        fig.update_layout(
            margin=dict(l=50, r=20, t=20, b=50),
            plot_bgcolor=BRAND_SURFACE,
            paper_bgcolor=BRAND_SURFACE,
            font=dict(family="Inter, sans-serif", color=BRAND_DARK),
            showlegend=False,
            yaxis_title="Demand MAPE (%)",
        )
        return fig
    else:
        return _empty_figure("Forecast Error Distribution", "Waiting for data...")

    fig = px.histogram(
        df, x=error_col, nbins=50,
        labels={error_col: f"Forecast Error ({unit})"},
        color_discrete_sequence=[BRAND_ACCENT],
    )

    fig.add_vline(x=0, line_dash="dash", line_color=BRAND_PRIMARY, opacity=0.7)

    fig.update_layout(
        margin=dict(l=50, r=20, t=20, b=50),
        plot_bgcolor=BRAND_SURFACE,
        paper_bgcolor=BRAND_SURFACE,
        font=dict(family="Inter, sans-serif", color=BRAND_DARK),
        showlegend=False,
        yaxis_title="Count",
    )
    return fig


@callback(
    Output("metrics-panel", "children"),
    Input("forecast-tabs", "value"),
    Input("demand-data", "data"),
    Input("price-data", "data"),
    Input("gold-demand-data", "data"),
    Input("gold-price-data", "data"),
    Input("gold-accuracy-data", "data"),
    Input("gold-interconnector-data", "data"),
)
def update_metrics_panel(
    tab, demand_json, price_json, gold_demand_json, gold_price_json, gold_accuracy_json, gold_interconnector_json
):
    """Update the model metrics panel."""
    if tab == "demand" and demand_json:
        df = pd.read_json(demand_json, orient="split")
        errors = df["forecast_error_mw"]
        actuals = df["actual_demand_mw"]
        unit = "MW"
    elif tab == "price" and price_json:
        df = pd.read_json(price_json, orient="split")
        errors = df["forecast_error_dollars"]
        actuals = df["actual_rrp"]
        unit = "$/MWh"
    elif tab == "pipeline" and gold_demand_json and gold_price_json and gold_accuracy_json:
        demand_df = pd.read_json(gold_demand_json, orient="split")
        price_df = pd.read_json(gold_price_json, orient="split")
        acc_df = pd.read_json(gold_accuracy_json, orient="split")
        inter_df = (
            pd.read_json(gold_interconnector_json, orient="split")
            if gold_interconnector_json else pd.DataFrame()
        )

        if demand_df.empty or price_df.empty or acc_df.empty:
            return html.P("No data available")

        demand_hours = demand_df["hour"].nunique() if "hour" in demand_df.columns else 0
        price_hours = price_df["hour"].nunique() if "hour" in price_df.columns else 0
        mean_mape = acc_df["demand_mape_pct"].mean()
        mean_price_mae = acc_df["price_mae"].mean()
        mean_lead = acc_df["avg_lead_time_minutes"].mean()
        mean_util = (
            inter_df["avg_utilisation_pct"].mean()
            if not inter_df.empty and "avg_utilisation_pct" in inter_df.columns
            else np.nan
        )

        metrics = [
            ("Gold Demand Rows", f"{len(demand_df):,}"),
            ("Gold Price Rows", f"{len(price_df):,}"),
            ("Gold Accuracy Rows", f"{len(acc_df):,}"),
            ("Distinct Demand Hours", f"{demand_hours:,}"),
            ("Distinct Price Hours", f"{price_hours:,}"),
            ("Avg AEMO Demand MAPE", f"{mean_mape:.2f}%"),
            ("Avg AEMO Price MAE", f"${mean_price_mae:.2f}/MWh"),
            ("Avg Forecast Lead Time", f"{mean_lead:.1f} min"),
            ("Avg Interconnector Util.", "N/A" if np.isnan(mean_util) else f"{mean_util:.1f}%"),
        ]

        return html.Table([
            html.Tbody([
                html.Tr([
                    html.Td(name, style={
                        "padding": "8px 12px", "fontSize": "13px",
                        "borderBottom": "1px solid #eee",
                    }),
                    html.Td(value, style={
                        "padding": "8px 12px", "fontSize": "13px",
                        "fontWeight": "600", "textAlign": "right",
                        "borderBottom": "1px solid #eee",
                    }),
                ]) for name, value in metrics
            ])
        ], style={
            "width": "100%",
            "backgroundColor": BRAND_SURFACE,
            "borderRadius": "8px",
            "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
            "border": f"1px solid {BRAND_BORDER}",
            "overflow": "hidden",
        })
    else:
        return html.P("No data available")

    mae = errors.abs().mean()
    rmse = np.sqrt((errors ** 2).mean())
    mape = (errors.abs() / actuals.abs().clip(lower=0.01)).mean() * 100
    bias = errors.mean()
    n_rows = len(df)

    metrics = [
        ("Mean Absolute Error (MAE)", f"{mae:.2f} {unit}"),
        ("Root Mean Square Error (RMSE)", f"{rmse:.2f} {unit}"),
        ("Mean Absolute % Error (MAPE)", f"{mape:.2f}%"),
        ("Bias (mean error)", f"{bias:+.2f} {unit}"),
        ("Number of predictions", f"{n_rows:,}"),
        ("Model version", str(df["model_version"].iloc[0]) if "model_version" in df.columns else "N/A"),
    ]

    return html.Table([
        html.Tbody([
            html.Tr([
                html.Td(name, style={
                    "padding": "8px 12px", "fontSize": "13px",
                    "borderBottom": "1px solid #eee",
                }),
                html.Td(value, style={
                    "padding": "8px 12px", "fontSize": "13px",
                    "fontWeight": "600", "textAlign": "right",
                    "borderBottom": "1px solid #eee",
                }),
            ]) for name, value in metrics
        ])
    ], style={
        "width": "100%",
        "backgroundColor": BRAND_SURFACE,
        "borderRadius": "8px",
        "boxShadow": "0 1px 3px rgba(15, 23, 42, 0.08)",
        "border": f"1px solid {BRAND_BORDER}",
        "overflow": "hidden",
    })


@callback(
    Output("predictions-table", "columnDefs"),
    Output("predictions-table", "rowData"),
    Input("forecast-tabs", "value"),
    Input("demand-data", "data"),
    Input("price-data", "data"),
    Input("gold-demand-data", "data"),
)
def update_predictions_table(tab, demand_json, price_json, gold_demand_json):
    """Update the predictions AG Grid table."""
    if tab == "demand" and demand_json:
        df = pd.read_json(demand_json, orient="split")
        df["settlement_date"] = pd.to_datetime(df["settlement_date"]).dt.strftime("%Y-%m-%d %H:%M")
        df = df.sort_values("settlement_date", ascending=False)

        col_defs = [
            {"field": "settlement_date", "headerName": "Time", "filter": True},
            {"field": "region_id", "headerName": "Region", "filter": True},
            {"field": "actual_demand_mw", "headerName": "Actual (MW)",
             "valueFormatter": {"function": "d3.format(',.0f')(params.value)"}},
            {"field": "predicted_demand_mw", "headerName": "Predicted (MW)",
             "valueFormatter": {"function": "d3.format(',.0f')(params.value)"}},
            {"field": "forecast_error_mw", "headerName": "Error (MW)",
             "valueFormatter": {"function": "d3.format('+,.0f')(params.value)"},
             "cellStyle": {"styleConditions": [
                 {"condition": "Math.abs(params.value) > 200", "style": {"color": "#dc3545", "fontWeight": "bold"}},
                 {"condition": "Math.abs(params.value) > 100", "style": {"color": "#fd7e14"}},
             ]}},
        ]
    elif tab == "price" and price_json:
        df = pd.read_json(price_json, orient="split")
        df["settlement_date"] = pd.to_datetime(df["settlement_date"]).dt.strftime("%Y-%m-%d %H:%M")
        df = df.sort_values("settlement_date", ascending=False)

        col_defs = [
            {"field": "settlement_date", "headerName": "Time", "filter": True},
            {"field": "region_id", "headerName": "Region", "filter": True},
            {"field": "actual_rrp", "headerName": "Actual ($/MWh)",
             "valueFormatter": {"function": "d3.format('$,.2f')(params.value)"}},
            {"field": "predicted_rrp", "headerName": "Predicted ($/MWh)",
             "valueFormatter": {"function": "d3.format('$,.2f')(params.value)"}},
            {"field": "forecast_error_dollars", "headerName": "Error ($/MWh)",
             "valueFormatter": {"function": "d3.format('+,.2f')(params.value)"},
             "cellStyle": {"styleConditions": [
                 {"condition": "Math.abs(params.value) > 50", "style": {"color": "#dc3545", "fontWeight": "bold"}},
                 {"condition": "Math.abs(params.value) > 20", "style": {"color": "#fd7e14"}},
             ]}},
        ]
    elif tab == "pipeline" and gold_demand_json:
        df = pd.read_json(gold_demand_json, orient="split")
        if df.empty:
            return [], []
        df["hour"] = pd.to_datetime(df["hour"]).dt.strftime("%Y-%m-%d %H:%M")
        df = df.sort_values("hour", ascending=False)

        col_defs = [
            {"field": "hour", "headerName": "Hour", "filter": True},
            {"field": "region_id", "headerName": "Region", "filter": True},
            {"field": "avg_demand_mw", "headerName": "Avg Demand (MW)",
             "valueFormatter": {"function": "d3.format(',.0f')(params.value)"}},
            {"field": "avg_rrp", "headerName": "Avg Price ($/MWh)",
             "valueFormatter": {"function": "d3.format('$,.2f')(params.value)"}},
            {"field": "air_temp_c", "headerName": "Air Temp (C)",
             "valueFormatter": {"function": "d3.format('.1f')(params.value)"}},
            {"field": "interval_count", "headerName": "Intervals",
             "valueFormatter": {"function": "d3.format(',.0f')(params.value)"}},
        ]
    else:
        return [], []

    return col_defs, df.to_dict("records")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
