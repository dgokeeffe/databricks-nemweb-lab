# Facilitator Guide: NEM Analytics Workshop

**Duration:** 60 minutes
**Format:** Live demo with interactive Q&A
**Audience:** Analysts with Excel/Power BI experience, minimal Databricks exposure

---

## Pre-Workshop Checklist (15 min before)

- [ ] SQL Warehouse running and warmed up
- [ ] Genie space accessible and tested
- [ ] Sample queries ready in SQL Editor tabs
- [ ] Dashboard deployed and working
- [ ] App deployed and accessible
- [ ] Power BI (optional) connected
- [ ] Screen sharing working
- [ ] Attendee list and names ready

---

## Session Timeline

| Time | Duration | Section | Key Demo |
|------|----------|---------|----------|
| 0:00 | 5 min | **Getting Started** | Databricks free edition signup |
| 0:05 | 5 min | **Curated Data** | Unity Catalog Explorer |
| 0:10 | 12 min | **AI/BI Genie** | Natural language queries ⭐ |
| 0:22 | 8 min | **Databricks Assistant** | SQL generation |
| 0:30 | 8 min | **Dashboard** | SQL Dashboard with KPIs |
| 0:38 | 7 min | **Databricks App** | Interactive analytics app |
| 0:45 | 10 min | **Workflow Discussion** | Map Excel/PBI to Databricks |
| 0:55 | 5 min | **Wrap-up** | Next steps & resources |

---

## Section 1: Getting Started (0:00 - 0:05)

**Duration:** 5 minutes
**Goal:** Show how easy it is to get started with Databricks

### Talking Points

> "Before we dive in, let me show you how anyone can get started with Databricks for free."

1. **Navigate to**: `databricks.com/try-databricks`

2. **Show the signup flow**:
   - Click "Get started free"
   - Choose "Community Edition" (free forever, no credit card)
   - Email signup takes ~2 minutes

3. **Key message**:
   > "Community Edition gives you a single-node cluster for learning and prototyping. For production work with your team, you'd use a full workspace like the one we're in today."

4. **Transition**:
   > "Now let's look at what you can do once you're in a Databricks workspace with your organisation's data."

---

## Section 2: Curated Data in Unity Catalog (0:05 - 0:10)

**Duration:** 5 minutes
**Goal:** Show governed, documented data assets

### Demo Steps

1. **Open Catalog Explorer**
   - Navigate to `workspace` → `nemweb_lab`
   - Show the tables: `curated_nem_prices`, `nemweb_dispatch_prices`, `nemweb_dispatch_regionsum`

2. **Click into `curated_nem_prices`**:
   - Show Schema tab with column descriptions
   - Show Sample Data tab
   - Mention lineage (data comes from AEMO)

### Talking Points

> "This is Unity Catalog - Databricks' governance layer. Notice every column has a description. This metadata powers AI features like Genie."

> "This is real AEMO data from the National Electricity Market - 6 months of dispatch prices and demand across all 5 NEM regions."

---

## Section 3: AI/BI Genie (0:10 - 0:22) ⭐

**Duration:** 12 minutes
**Goal:** Demonstrate natural language analytics - the "wow moment"

### Demo Steps

1. **Open Genie Space**: "NEM Trading Analytics"

2. **Ask progressively complex questions**:

   **Question 1** (simple):
   > "What was the average spot price in NSW over the last 7 days?"

   - Wait for response
   - Click "Show SQL" to reveal the query
   - Point out: "It knew RRP means price, NSW1 is the region code"

   **Question 2** (comparison):
   > "How does that compare to QLD?"

   - Shows Genie maintains context

   **Question 3** (visualization):
   > "Show me a line chart of daily average price by region for the last 30 days"

   **Question 4** (domain-specific):
   > "When were the top 5 demand peaks in VIC, and what were the prices?"

3. **Invite 1-2 audience questions** (if time)

### Talking Points

> "Genie isn't magic - it's powered by the column descriptions we set up. The better your metadata, the better Genie performs."

> "Notice I can always see the SQL. This isn't a black box."

---

## Section 4: Databricks Assistant (0:22 - 0:30)

**Duration:** 8 minutes
**Goal:** Show AI-assisted SQL development

### Demo Steps

1. **Open SQL Editor**

2. **Ask Assistant**:
   > "Write a query to compare peak vs off-peak average prices by region for the last week"

3. **Run the generated SQL**

4. **Ask for refinement**:
   > "Add the percentage difference between peak and off-peak"

### Talking Points

> "Assistant is for analysts writing SQL. Genie is for end users asking business questions."

> "Think of it as pair programming - it drafts, you verify."

---

## Section 5: SQL Dashboard (0:30 - 0:38)

**Duration:** 8 minutes
**Goal:** Show pre-built dashboard with KPIs and charts

### Demo Steps

1. **Open the NEM Price Analytics dashboard**

2. **Walk through the widgets**:
   - **KPI counters**: Avg price, peak demand, spike count
   - **Price trend**: Line chart by region over time
   - **Regional summary**: Table with volatility metrics
   - **Peak vs off-peak**: Comparison bar chart

3. **Show interactivity**:
   - Click on a region to filter
   - Hover for tooltips
   - Show the underlying SQL (click widget menu)

4. **Mention scheduling**:
   > "This dashboard can be scheduled to refresh automatically and email reports"

### Talking Points

> "Anyone with SQL access can build these dashboards - no BI tool license required."

> "The data is live from the same curated view Genie uses. One source, many views."

---

## Section 6: Databricks App (0:38 - 0:45)

**Duration:** 7 minutes
**Goal:** Show custom interactive application

### Demo Steps

1. **Open the NEM Analytics App** (in browser)

2. **Show the features**:
   - Regional KPI cards with live prices
   - Date range selector
   - Region filter
   - Interactive charts

3. **Explain the architecture**:
   > "This is a Python Dash app hosted on Databricks. It queries the same curated_nem_prices view."

4. **Show responsiveness**:
   - Change date range → charts update
   - Filter regions → KPIs recalculate

### Talking Points

> "Databricks Apps let you build custom interfaces for your users. This one took about an hour to build."

> "The app uses the same SQL Warehouse and data as everything else - no data copying."

### If App Not Deployed

Show the code structure instead:
- `app/app.py` - the main application
- `app/app.yaml` - deployment config
- Explain it can be deployed in minutes

---

## Section 7: Workflow Discussion (0:45 - 0:55)

**Duration:** 10 minutes
**Goal:** Map current Excel/Power BI workflows to Databricks

### Facilitation Approach

1. **Ask the audience**:
   > "What are 2-3 reports or analyses you currently do in Excel that involve NEM data?"

2. **For each workflow mentioned, discuss**:
   - Where does the data come from today?
   - How often is it refreshed?
   - Who else needs this analysis?

3. **Map to Databricks**:

   | Current Workflow | Databricks Solution |
   |------------------|---------------------|
   | Excel pivot table | Genie or Dashboard |
   | Manual data pull | Scheduled pipeline |
   | Emailing spreadsheets | Shared dashboard |
   | Power BI report | Same report, governed source |
   | Custom Excel model | Databricks App |

### Talking Points

> "Databricks isn't replacing Excel - it's replacing the messy data prep before Excel."

> "The goal is: one governed source, many consumption options."

---

## Section 8: Wrap-up (0:55 - 1:00)

**Duration:** 5 minutes
**Goal:** Clear next steps

### Key Messages

1. **What we covered**:
   - Governed data in Unity Catalog
   - Natural language queries with Genie
   - AI-assisted SQL with Assistant
   - Self-service dashboards
   - Custom analytics apps

2. **What Databricks brings**:
   - Single source of truth
   - AI augmentation for everyone
   - Governance and lineage built-in
   - Connect your existing tools (Excel, Power BI)

3. **Next steps**:
   - Try Databricks Community Edition: `databricks.com/try-databricks`
   - Identify 1-2 datasets for a pilot
   - Schedule follow-up hands-on session

### Resources to Share

- Databricks Free: https://databricks.com/try-databricks
- Genie Docs: https://docs.databricks.com/genie/
- Apps Docs: https://docs.databricks.com/apps/

---

## Backup Content (if time permits)

### Price Spike Analysis

```sql
SELECT date, region_id, COUNT(*) as spike_intervals,
       ROUND(MAX(rrp), 2) as max_price
FROM workspace.nemweb_lab.curated_nem_prices
WHERE rrp > 300
GROUP BY date, region_id
ORDER BY max_price DESC;
```

### Metric Views (Optional Section)

If audience is technical, show the metric view:
```sql
SELECT region_id, date, avg_price, price_volatility
FROM workspace.nemweb_lab.mv_nem_price_metrics
WHERE date >= current_date() - INTERVAL 7 DAYS;
```

> "Metric views define calculations once, use everywhere."

### Power BI Connection

If someone asks about Power BI:
- Show `docs/powerbi_connection.md`
- Explain: "Power BI connects directly via Partner Connect or ODBC"

### Generator Metadata

If someone asks about generation data:
- Show `nemweb_lab.nem_generators` table
- Contains: station names, fuel types, capacity, location

---

## Pre-Workshop Setup (for Facilitator)

### Deploy Dashboard

1. Open SQL Editor
2. Run queries from `dashboard/dashboard_queries.sql`
3. Create dashboard with widgets (see `dashboard/README.md`)
4. Test all visualizations load correctly

### Deploy App

```bash
# Create app
databricks apps create nem-analytics

# Import to workspace first
databricks workspace import-dir app /Workspace/Users/<your-email>/nem-analytics-app

# Deploy
databricks apps deploy nem-analytics --source-code-path /Workspace/Users/<your-email>/nem-analytics-app
```

Configure SQL Warehouse in app settings.

### Setup Genie Space

Follow `docs/genie_setup.md`:
1. Create space "NEM Trading Analytics"
2. Add datasets: `curated_nem_prices`, `mv_nem_price_metrics`
3. Add context instructions
4. Add example questions
5. Test with sample questions

---

## Post-Workshop

- [ ] Share recording (if recorded)
- [ ] Send links to resources
- [ ] Share dashboard/app URLs
- [ ] Follow up on identified pilot datasets
- [ ] Schedule hands-on session if interest
