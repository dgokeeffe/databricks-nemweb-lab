# Demo Outline: Databricks for Trading Analytics at AGL

**Duration**: 60 minutes
**Format**: Demo-only (presenter drives, audience watches and interacts)
**Audience**: AGL Trading Analytics team

---

## Pre-Demo Checklist

- [ ] Databricks workspace accessible
- [ ] `workspace.nemweb_lab.curated_nem_prices` table loaded with sample data
- [ ] `workspace.nemweb_lab.mv_nem_price_metrics` metric view created
- [ ] Genie space "AGL Trading Analytics - NEM" configured
- [ ] SQL Editor open with sample queries ready
- [ ] (Optional) Power BI Desktop with Databricks connection tested

---

## Section 1: Context (0-5 min)

**Goal**: Make attendees comfortable; relate to their Excel/Power BI reality

### Key Messages

- "You mostly live in Excel and Power BI today - that's expected and won't change overnight"
- AGL is driving more data & AI capabilities across the business
- Databricks is:
  - Where NEM and trading data is ingested, cleaned, and governed
  - Where AI tools help you ask questions and write queries
  - A backend that feeds Power BI - not a replacement for it

### Demo Actions

1. Show the Databricks workspace landing page
2. Point out key areas: Catalog, SQL Editor, Genie

### Outcome

Mental model: "Databricks behind the scenes; Excel/Power BI + AI at the front"

---

## Section 2: Curated NEM Data (5-12 min)

**Goal**: Show a familiar dataset framed as "the table you wish you had in Power BI"

### Demo Actions

1. **Catalog Explorer**: Navigate to `workspace.nemweb_lab.curated_nem_prices`
2. **Show schema**: Click through columns, highlight comments
   - `interval_start` - 5-minute NEM dispatch intervals
   - `region_id` - NSW1, QLD1, VIC1, SA1, TAS1
   - `rrp` - Regional Reference Price (the spot price)
   - `demand_mw` - Scheduled demand
   - `is_peak` - Peak period flag (17:00-21:00)
3. **Sample data**: Preview some rows
4. **SQL Editor**: Run 7-day summary query

```sql
SELECT
  region_id,
  COUNT(*) AS rows,
  AVG(rrp) AS avg_price,
  AVG(demand_mw) AS avg_demand
FROM workspace.nemweb_lab.curated_nem_prices
WHERE date >= current_date() - INTERVAL 7 DAYS
GROUP BY region_id;
```

### Key Message

"This replaces multiple CSV exports and one-off extracts you currently juggle. One governed source for everyone."

---

## Section 3: AI/BI Genie (12-25 min)

**Goal**: Deliver the main "wow" moment with natural language queries

### Demo Actions

1. **Open Genie**: Navigate to "AGL Trading Analytics - NEM" space
2. **Explain Genie**:
   - AI-powered BI: ask questions in natural language
   - Uses your metadata and examples to generate SQL
   - Results shown as tables or visualisations
3. **Scripted questions** (type these in):

   **Question 1**: "What was the average spot price in NSW over the last 7 days?"
   - Wait for response, show the answer

   **Question 2**: "How does that compare to QLD over the same period?"
   - Show comparison capability

   **Question 3**: "Show me a line chart of daily average price by region for the last 30 days"
   - Show visualisation capability

   **Question 4**: "When were the top 5 demand peaks in VIC this week, and what were the prices?"
   - Show detail drill-down

4. **Show the SQL**: Click to reveal generated query
5. **Audience participation**: Invite 1-2 questions from the room
   - Suggestions: "Which region had the highest volatility?"
   - "What was the price during the highest demand period?"

### Key Messages

- "This is like asking Power BI a question without building a new report"
- "Trading Analytics can curate which data Genie sees, so it stays safe and meaningful"

---

## Section 4: Databricks Assistant (25-35 min)

**Goal**: Show how analysts can get SQL written for them, then refine

### Demo Actions

1. **Open SQL Editor**: Ensure `agldata` catalog is selected
2. **First prompt** (use Assistant):

   > "Write a SQL query over workspace.nemweb_lab.curated_nem_prices that, for the last 7 days, returns average price, price volatility, and peak demand by region, ordered by average price descending"

3. **Review generated SQL**: Talk through the query structure
4. **Run the query**: Show results table
5. **Refinement prompt**:

   > "Modify this query so it only includes NSW1 and QLD1 and shows daily values instead of overall 7-day aggregates"

6. **Show updated SQL**: Point out the changes made
7. **Run refined query**: Show results

### Key Messages

- "You don't need to remember every SQL detail - describe what you want, then adjust"
- "SQL from Assistant can feed new metric views, dashboards, or Power BI"

---

## Section 5: Metric Views + Dashboard (35-45 min)

**Goal**: Introduce the semantic layer concept and show a dashboard

### Metric Views Explanation (2-3 min)

**Requirements**: SQL Warehouse (Serverless Environment Version 4+) or DBR 17.2+

**Key explanation**: "Metric views define measures once - like avg price, volatility, peak demand - so everyone reuses the same definitions in SQL, dashboards, Genie, and Power BI"

### Demo Actions

1. **Catalog Explorer**: Navigate to `workspace.nemweb_lab.mv_nem_price_metrics`
2. **Show definition**: Point out measures and dimensions
3. **Run metric view query**:

```sql
SELECT
  region_id,
  date,
  avg_price,
  price_volatility,
  peak_demand_mw
FROM workspace.nemweb_lab.mv_nem_price_metrics
WHERE date >= current_date() - INTERVAL 7 DAYS
ORDER BY avg_price DESC;
```

4. **Create/show dashboard** (pre-built recommended):
   - Line chart: daily `avg_price` by region
   - Bar chart: `price_volatility` by region
   - KPI card: `peak_price` over last 24h

### Key Message

"This is the same logic you might re-implement in DAX or Excel formulas, but now centralised. Dashboards, Genie, and Power BI all share the same semantics."

---

## Section 6: Map Current Workflows (45-55 min)

**Goal**: Anchor everything in how they actually work today

### Discussion Prompts

Ask the audience:
- "What are 2-3 recurring Excel or Power BI reports you run with NEM data?"
- "Where does that data currently come from?"
- "How often do you refresh it?"

### For Each Report Mentioned, Sketch:

| Current State | With Databricks |
|---------------|-----------------|
| Source: CSV exports, manual downloads | Curated table (`curated_nem_prices`) |
| Logic: Excel formulas, DAX measures | Metric view (`mv_nem_price_metrics`) |
| Consumption: Static Excel/PBIX files | Power BI DirectQuery, Genie ad-hoc |

### Optional: Power BI Demo (if time)

1. Show Power BI connected to Databricks SQL endpoint
2. Point out connection settings briefly
3. Show a simple visual using the metric view

**Message**: "Live governed data, not files"

---

## Section 7: Wrap-up & Next Steps (55-60 min)

### Summary Points

1. **Databricks as governed, AI-powered data layer** - not a replacement for Excel/Power BI
2. **Genie for natural-language questions** - ask without building reports
3. **Assistant for SQL help** - describe what you want, refine as needed
4. **Metric views as single source of KPIs** - consistent definitions everywhere

### Proposed Next Steps

1. **Working session (60-90 min)** with Trading Analytics leads to:
   - Choose 2-3 tables to curate (NEM + internal)
   - Design first 1-2 production metric views
   - Write 10-15 canonical Genie questions for a production space

2. **Hands-on lab options**:
   - "Genie for Trading Analytics" - everyone using Genie
   - "Power BI + Databricks" - build a report on the metric view

### Close

- Thank attendees for their time
- Collect feedback and questions
- Share any follow-up resources

---

## Backup: Common Questions

**Q: Can we connect our existing Power BI reports?**
A: Yes - Power BI connects directly to Databricks SQL. Existing reports can be migrated to use governed tables.

**Q: How does this handle real-time data?**
A: The curated tables can be updated in near-real-time. For this demo we use batch data, but streaming is supported.

**Q: Who maintains the metric views?**
A: Typically a data team or analytics lead defines and maintains metric views. Once defined, everyone consumes them consistently.

**Q: Is Genie secure?**
A: Yes - Genie only accesses data you explicitly add to the space. It respects Unity Catalog permissions.
