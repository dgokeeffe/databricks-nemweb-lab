# NEM Price Analytics App

A Dash application for NEM price analytics, deployed as a Databricks App.

## Features

- Real-time data from `curated_nem_prices` view
- Regional KPI cards (avg price, max, volatility)
- Daily price trend chart
- Price volatility comparison
- Peak vs off-peak analysis

## Deployment

### 1. Deploy via CLI

```bash
# Create the app
databricks apps create nem-analytics

# Deploy from workspace path
databricks apps deploy nem-analytics \
  --source-code-path /Workspace/Users/<your-email>/databricks-nemweb-analyst-lab/app
```

### 2. Configure Resources

After deployment, configure the SQL Warehouse:

1. Open the app in Databricks
2. Go to **Settings** > **Resources**
3. Select a SQL Warehouse
4. Click **Save** and **Restart**

### 3. Access the App

The app URL will be:
```
https://nem-analytics-<workspace-id>.cloud.databricks.com
```

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally (uses sample data)
python app.py

# Open http://localhost:8050
```

## Environment Variables

Set by Databricks Apps automatically:

| Variable | Description |
|----------|-------------|
| `DATABRICKS_WAREHOUSE_ID` | SQL Warehouse ID |
| `DATABRICKS_HOST` | Workspace hostname |
| `DATABRICKS_TOKEN` | Authentication token |

## Data Source

The app queries:
```
workspace.nemweb_lab.curated_nem_prices
```

This view joins:
- `nemweb_dispatch_prices` (RRP)
- `nemweb_dispatch_regionsum` (demand)

## Customization

### Change Date Range

Edit the `query_nem_data()` function to adjust the default date range.

### Add Metrics

Add new charts by:
1. Adding a new `dcc.Graph` to the layout
2. Creating a callback that queries and visualizes the data

### Theming

Modify the header colors and styles in the `app.layout` definition.

## Files

| File | Purpose |
|------|---------|
| `app.py` | Main Dash application |
| `app.yaml` | Databricks App configuration |
| `requirements.txt` | Python dependencies |
