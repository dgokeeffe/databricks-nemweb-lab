# NEMWEB Price Dashboard - Databricks App

A real-time dashboard for Australian National Electricity Market (NEM) prices.

## Features

- Live price cards for all 5 NEM regions (NSW, VIC, QLD, SA, TAS)
- Price history chart with configurable time range
- Demand comparison by region
- Price distribution analysis
- Auto-refresh every 30 seconds
- Price spike highlighting (warning/alert/critical thresholds)

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
python app.py

# Open http://localhost:8050
```

## Deploy to Databricks

### Option 1: Using Databricks CLI

```bash
# 1. Create the app
databricks apps create nemweb-prices

# 2. Deploy from local directory
databricks apps deploy nemweb-prices --source-code-path ./app

# 3. Start the app
databricks apps start nemweb-prices
```

### Option 2: Using Workspace UI

1. Go to **Compute** > **Apps**
2. Click **Create App** > **Create a custom app**
3. Name: `nemweb-prices`
4. Upload files from this directory
5. Click **Deploy**

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NEMWEB_DATA_SOURCE` | Data source: `sample` or `delta` | `sample` |
| `NEMWEB_DELTA_TABLE` | Delta table name for price data | `nemweb_bronze` |
| `DATABRICKS_WAREHOUSE_ID` | SQL Warehouse ID (for Delta source) | - |

### Using Real Data

To display real NEMWEB data instead of sample data:

1. Run the Lakeflow pipeline to populate `nemweb_bronze` table
2. Update `app.yaml`:
   ```yaml
   env:
     - name: NEMWEB_DATA_SOURCE
       value: "delta"
     - name: NEMWEB_DELTA_TABLE
       value: "your_catalog.your_schema.nemweb_bronze"
     - name: DATABRICKS_WAREHOUSE_ID
       value: "your-warehouse-id"
   ```
3. Redeploy the app

## Price Thresholds

| Level | Price ($/MWh) | Meaning |
|-------|---------------|---------|
| Normal | < $100 | Typical market conditions |
| Elevated | $100 - $300 | Above average, watch closely |
| High | $300 - $1,000 | Significant price event |
| Critical | > $1,000 | Market stress, possible intervention |

Note: Australian NEM prices can spike to $15,000/MWh (market price cap) during extreme events.

## Architecture

```
app/
├── app.py           # Main Dash application
├── app.yaml         # Databricks App runtime config
├── requirements.txt # Python dependencies
└── README.md        # This file
```

## Data Flow

```
NEMWEB API
    ↓
Custom Data Source (nemweb_datasource.py)
    ↓
Lakeflow Pipeline (DLT)
    ↓
Delta Table (nemweb_bronze)
    ↓
Databricks App (this app)
    ↓
User Browser
```

## Screenshots

The dashboard displays:
- **Price Cards**: Current price and status for each region
- **Price Chart**: Time-series line chart with threshold markers
- **Demand Chart**: Bar chart of current demand by region
- **Distribution**: Box plot showing price ranges
- **Data Table**: Detailed price data with pagination
