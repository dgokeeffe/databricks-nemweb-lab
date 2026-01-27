# Facilitator Guide: NEM Analytics Workshop

**Duration:** 60 minutes
**Format:** Live demo with interactive Q&A
**Audience:** Analysts with Excel/Power BI experience, minimal Databricks exposure

---

## Pre-Workshop Checklist (15 min before)

- [ ] SQL Warehouse running and warmed up
- [ ] Genie space accessible and tested
- [ ] Sample queries ready in SQL Editor tabs
- [ ] Power BI (optional) connected and showing data
- [ ] Screen sharing working
- [ ] Attendee list and names ready

---

## Session Timeline

| Time | Duration | Section | Key Demo |
|------|----------|---------|----------|
| 0:00 | 5 min | **Getting Started** | Databricks Community Edition signup |
| 0:05 | 7 min | **Curated Data** | Unity Catalog Explorer |
| 0:12 | 13 min | **AI/BI Genie** | Natural language queries ⭐ |
| 0:25 | 10 min | **Databricks Assistant** | SQL generation in editor |
| 0:35 | 10 min | **Metric Views** | Semantic layer + dashboard |
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

## Section 2: Curated Data in Unity Catalog (0:05 - 0:12)

**Duration:** 7 minutes
**Goal:** Show governed, documented data assets

### Demo Steps

1. **Open Catalog Explorer**
   - Navigate to `workspace` → `nemweb_lab`
   - Show the tables: `curated_nem_prices`, `nemweb_dispatch_prices`, `nemweb_dispatch_regionsum`

2. **Click into `curated_nem_prices`**:
   - Show Schema tab with column descriptions
   - Show Sample Data tab
   - Show Lineage tab (if available)

### Talking Points

> "This is Unity Catalog - Databricks' governance layer. Notice every column has a description. This metadata powers AI features like Genie."

> "Unlike a shared drive or local Excel files, this data has:
> - Version history (time travel)
> - Access controls (who can see what)
> - Lineage (where did this data come from)"

### Questions to Expect

- "Who manages these descriptions?" → Data stewards or engineers
- "Can we connect Excel to this?" → Yes, via ODBC or export

---

## Section 3: AI/BI Genie (0:12 - 0:25) ⭐

**Duration:** 13 minutes
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

   - Shows Genie maintains context from previous question

   **Question 3** (visualization):
   > "Show me a line chart of daily average price by region for the last 30 days"

   - Demonstrates chart generation

   **Question 4** (domain-specific):
   > "When were the top 5 demand peaks in VIC, and what were the prices?"

   - Shows understanding of NEM terminology

3. **Invite audience questions**:
   > "What would YOU like to ask about NEM prices? Give me a question in the chat."

   - Take 2-3 real questions from attendees
   - If Genie struggles, show how to rephrase

### Talking Points

> "Genie isn't magic - it's powered by the column descriptions and context we set up. The better your metadata, the better Genie performs."

> "Notice I can always see the SQL. This isn't a black box - you can verify, modify, or save these queries."

### Troubleshooting

- **Genie gives wrong answer**: "Let me rephrase that..." (shows learning opportunity)
- **Slow response**: "The first query warms up the warehouse..."
- **Attendee question fails**: "Great question - this shows where we need better metadata"

---

## Section 4: Databricks Assistant (0:25 - 0:35)

**Duration:** 10 minutes
**Goal:** Show AI-assisted SQL development

### Demo Steps

1. **Open SQL Editor**

2. **Open Assistant panel** (usually right sidebar)

3. **Ask Assistant to write SQL**:
   > "Write a query to compare peak vs off-peak average prices by region for the last week"

4. **Show the generated SQL**, run it

5. **Ask for refinement**:
   > "Add a column showing the percentage difference between peak and off-peak"

6. **Show "Explain" feature**:
   - Highlight complex SQL from `02_sample_queries.sql`
   - Ask Assistant to explain it

### Talking Points

> "Assistant is different from Genie. Genie is for end users asking business questions. Assistant is for analysts and developers writing SQL."

> "Think of it as pair programming - it drafts, you verify and refine."

---

## Section 5: Metric Views (0:35 - 0:45)

**Duration:** 10 minutes
**Goal:** Show semantic layer concept

### Demo Steps

1. **Show metric view definition** (read-only):
   - Open `03_metric_view.sql` or show in Catalog
   - Point out dimensions vs measures

2. **Query the metric view**:
   ```sql
   SELECT region_id, date, avg_price, price_volatility
   FROM workspace.nemweb_lab.mv_nem_price_metrics
   WHERE date >= current_date() - INTERVAL 7 DAYS
   ORDER BY date;
   ```

3. **Show a dashboard** (if prepared):
   - Pre-built visualizations using the metric view
   - Same measures, consistent everywhere

### Talking Points

> "A metric view is like a single source of truth for calculations. 'Average price' means the same thing whether you're in Genie, SQL, a dashboard, or Power BI."

> "In Excel, you might have 5 different spreadsheets with 5 different formulas for 'average price'. Metric views solve that."

---

## Section 6: Workflow Discussion (0:45 - 0:55)

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
   | Current | Databricks Equivalent |
   |---------|----------------------|
   | Excel pivot table | SQL query or Genie |
   | Manual data pull | Scheduled pipeline |
   | Emailing spreadsheets | Shared dashboard |
   | Power BI report | Same, but governed source |

### Talking Points

> "Databricks isn't replacing Excel - it's replacing the messy data prep before Excel. You still get your final numbers in whatever tool you prefer."

> "The goal is: one governed source, many consumption options."

---

## Section 7: Wrap-up (0:55 - 1:00)

**Duration:** 5 minutes
**Goal:** Clear next steps

### Key Messages

1. **What we covered**:
   - Governed data in Unity Catalog
   - Natural language queries with Genie
   - AI-assisted SQL with Assistant
   - Semantic layer with Metric Views

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

- Databricks Community Edition: https://databricks.com/try-databricks
- Genie Documentation: https://docs.databricks.com/genie/
- Metric Views: https://docs.databricks.com/metric-views/

---

## Backup Content (if time permits or questions arise)

### Price Spike Analysis

```sql
SELECT date, region_id, COUNT(*) as spike_intervals,
       ROUND(MAX(rrp), 2) as max_price
FROM workspace.nemweb_lab.curated_nem_prices
WHERE rrp > 300
GROUP BY date, region_id
ORDER BY max_price DESC;
```

### Power BI Connection Demo

If someone asks about Power BI:
1. Show `docs/powerbi_connection.md`
2. Explain: "Power BI connects directly to these tables via Partner Connect or ODBC"

### Generator Metadata

If someone asks about generation data:
- Show `nemweb_lab.nem_generators` table
- Contains: station names, fuel types, capacity, location

---

## Post-Workshop

- [ ] Share recording (if recorded)
- [ ] Send links to resources
- [ ] Follow up on identified pilot datasets
- [ ] Schedule hands-on session if interest
