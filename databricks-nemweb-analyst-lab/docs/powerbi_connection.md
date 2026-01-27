# Power BI Connection to Databricks

This guide covers connecting Power BI to Databricks for the AGL Trading Analytics demo.

## Prerequisites

- Power BI Desktop installed (latest version recommended)
- Databricks SQL Warehouse running
- Personal Access Token or OAuth configured
- Network access from Power BI to Databricks workspace

## Connection Information

You'll need the following from your Databricks workspace:

| Setting | Where to Find |
|---------|---------------|
| **Server Hostname** | SQL Warehouse > Connection Details > Server hostname |
| **HTTP Path** | SQL Warehouse > Connection Details > HTTP path |
| **Catalog** | `agldata` (for this demo) |
| **Schema** | `trading` |

## Step-by-Step Connection

### 1. Open Power BI Desktop

Launch Power BI Desktop on your machine.

### 2. Get Data

1. Click **Get Data** from the Home ribbon
2. Search for "Databricks" in the connector list
3. Select **Azure Databricks** (or **Databricks** depending on your version)
4. Click **Connect**

### 3. Enter Connection Details

In the connection dialog:

```
Server Hostname: <your-workspace>.cloud.databricks.com
HTTP Path: /sql/1.0/warehouses/<warehouse-id>
```

**Data Connectivity Mode**:
- Choose **DirectQuery** for live data (recommended for demo)
- Or **Import** for cached data

Click **OK**.

### 4. Authentication

Choose your authentication method:

**Personal Access Token (PAT)**:
1. Select "Personal Access Token"
2. Paste your Databricks PAT
3. Click **Connect**

**Azure Active Directory (if configured)**:
1. Select "Azure Active Directory"
2. Sign in with your AAD credentials

### 5. Select Tables

In the Navigator:

1. Expand the `agldata` catalog
2. Expand the `trading` schema
3. Select:
   - `curated_nem_prices` (raw data)
   - `mv_nem_price_metrics` (metric view)
4. Click **Load** or **Transform Data**

## Building Demo Visuals

### Visual 1: Daily Average Price by Region (Line Chart)

1. Add a **Line Chart** visual
2. Configure:
   - **X-axis**: `date`
   - **Y-axis**: `avg_price` (from metric view) or `AVG(rrp)` (from table)
   - **Legend**: `region_id`
3. Filter to last 7-30 days

### Visual 2: Price Volatility by Region (Bar Chart)

1. Add a **Clustered Bar Chart** visual
2. Configure:
   - **Y-axis**: `region_id`
   - **X-axis**: `price_volatility` or `STDEV(rrp)`
3. Sort by volatility descending

### Visual 3: Peak Price KPI Card

1. Add a **Card** visual
2. Configure:
   - **Fields**: `peak_price` or `MAX(rrp)`
3. Add filter for last 24 hours

### Visual 4: Regional Summary Table

1. Add a **Table** visual
2. Add columns:
   - `region_id`
   - `avg_price`
   - `price_volatility`
   - `peak_demand_mw`
3. Filter to last 7 days

## Using the Metric View

The metric view `mv_nem_price_metrics` provides pre-calculated measures:

| Measure | Description |
|---------|-------------|
| `avg_price` | Average spot price |
| `price_volatility` | Price standard deviation |
| `peak_price` | Maximum price |
| `avg_demand_mw` | Average demand |
| `peak_demand_mw` | Maximum demand |

**Benefit**: These measures are calculated consistently whether accessed from Power BI, Genie, or SQL - no need to recreate DAX formulas.

## Demo Talking Points

When showing Power BI to the audience:

1. **"Live governed data"**: Emphasise that Power BI is reading directly from Databricks, not from exported files

2. **"Same definitions everywhere"**: The metric view ensures avg_price in Power BI matches avg_price in Genie and SQL

3. **"No manual refresh"**: With DirectQuery, data is always current (subject to source refresh)

4. **"Unity Catalog governance"**: Access controls and lineage are maintained - Power BI respects permissions

## Troubleshooting

### Connection fails

- Verify SQL Warehouse is running (check Databricks UI)
- Check network connectivity (firewall/VPN)
- Verify PAT hasn't expired
- Try generating a new PAT

### Slow queries

- Consider Import mode instead of DirectQuery for demos
- Ensure SQL Warehouse is appropriately sized
- Check if table statistics are current

### Tables not visible

- Verify catalog/schema names are correct
- Check Unity Catalog permissions for your user
- Ensure tables exist and have data

### Metric view not working

- Metric views require DBR 17.2+
- May need specific Power BI connector version
- Fall back to using the base table with Power BI measures if needed

## Alternative: Databricks SQL Connector for Python

For advanced scenarios, data can be extracted via Python:

```python
from databricks import sql
import pandas as pd

with sql.connect(
    server_hostname="<workspace>.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/<warehouse-id>",
    access_token="<pat>"
) as connection:
    df = pd.read_sql(
        "SELECT * FROM workspace.nemweb_lab.mv_nem_price_metrics WHERE date >= current_date() - INTERVAL 7 DAYS",
        connection
    )
```

This can be used to export data for offline analysis or other BI tools.

## Resources

- [Databricks Power BI Integration Docs](https://docs.databricks.com/partners/bi/power-bi.html)
- [Unity Catalog with Power BI](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Databricks SQL Connector](https://docs.databricks.com/dev-tools/python-sql-connector.html)
