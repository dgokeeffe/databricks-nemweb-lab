# PRD: Databricks Trading Analytics Intro (Demo‑Only) for AGL

## 1. Overview

### 1.1 Title

**Databricks for Trading Analytics at AGL: NEM Insights with AI, Notebooks, and Power BI**

### 1.2 Owner

- Databricks Solutions Architect (workshop presenter)

### 1.3 Audience

- AGL Trading Analytics team (incl. Manager Trading Analytics).  
- Typical profile:
  - Strong domain expertise in NEM, trading, and risk.
  - Heavy users of Excel and Power BI.
  - Mixed comfort with SQL; minimal Databricks / Spark experience. [istart.com](https://istart.com.au/news-items/agl-powers-up-for-data-and-ai-future/)

### 1.4 Session Type

- **Demo‑only**, no attendee hands‑on (first exposure session).
- Interactive via Q&A and audience‑driven Genie questions.

### 1.5 Duration

- 60 minutes (strict)

### 1.6 Delivery Mode

- Live (in‑person or video) screen‑share of a Databricks workspace.
- You drive; participants watch and interact verbally/through chat.

### 1.7 Platform Assumptions

- Databricks workspace with:
  - Unity Catalog enabled. [datacamp](https://www.datacamp.com/tutorial/databricks-unity-catalog-guide)
  - Delta Lake available. [datacamp](https://www.datacamp.com/tutorial/delta-lake)
  - Databricks AI features enabled:
    - AI/BI Genie. [docs.databricks](https://docs.databricks.com/aws/en/genie/)
    - Databricks Assistant in SQL Editor and notebooks. [learn.microsoft](https://learn.microsoft.com/en-us/azure/databricks/notebooks/databricks-assistant-faq)
  - Databricks SQL (or SQL Warehouse) for BI connectivity.
- (Optional) Power BI Desktop installed on presenter’s machine and able to connect to Databricks. [beyondkey](https://www.beyondkey.com/blog/databricks-integration-with-power-bi/)

***

## 2. Goals and Success Criteria

### 2.1 Business Goals

- Show Trading Analytics how Databricks can be the **governed, AI‑augmented data layer** for NEM and trading analytics while keeping Excel/Power BI in the picture. [databricks](https://www.databricks.com/blog/enabling-business-users-databricks)
- Align with AGL’s broader **data & AI literacy** and cloud/data transformation efforts. [news.microsoft](https://news.microsoft.com/en-au/features/agl-transforms-200-applications-goes-all-in-on-cloud-and-sets-up-for-sustained-success/)
- Create appetite for follow‑up **hands‑on** sessions focused on Genie or Power BI + Databricks.

### 2.2 Learning Objectives

By the end of the session, attendees should:

1. Understand, at a high level, how Databricks One and the lakehouse support NEM‑driven trading analytics. [docs.databricks](https://docs.databricks.com/aws/en/lakehouse-architecture/)
2. Recognise a curated NEM table in Unity Catalog as a **single, governed source** instead of ad‑hoc CSVs. [docs.databricks](https://docs.databricks.com/aws/en/data-governance/unity-catalog/get-started)
3. See how AI/BI Genie can answer typical Trading Analytics questions in natural language over curated data. [learn.microsoft](https://learn.microsoft.com/en-us/azure/databricks/genie/)
4. See how Databricks Assistant can help generate/refine SQL for new metrics/queries without deep SQL expertise. [docs.databricks](https://docs.databricks.com/aws/en/notebooks/use-databricks-assistant)
5. Grasp that metric views define official KPIs (avg price, volatility, peak demand) once and can feed Genie, dashboards, and Power BI. [advancinganalytics.co](https://www.advancinganalytics.co.uk/blog/simplifying-analytics-with-metric-views-in-databricks)
6. Understand that Power BI can connect directly to Databricks as a live, governed data source. [cdata](https://www.cdata.com/kb/tech/databricks-powerbi-gateway.rst)

### 2.3 Success Criteria

- During the Genie segment, at least **two questions** come from the audience and are answered live.
- Participants can name at least **three current Excel/Power BI use cases** that could be backed by Databricks (verbally or via chat).
- Agreement to:
  - Identify first curated tables/metric views after the session.
  - Consider a 60–90 min **hands‑on** follow‑up workshop.

***

## 3. Scope

### 3.1 In Scope

- High‑level explanation of Databricks lakehouse in **business terms** (no architecture deep dive). [lakefs](https://lakefs.io/blog/databricks-lakehouse/)
- Demos (you driving) of:
  - Curated NEM‑style table in Unity Catalog.
  - AI/BI Genie over that table.
  - Databricks Assistant in **SQL Editor** (notebooks kept simple).
  - Unity Catalog metric view for NEM KPIs.
  - Simple Databricks dashboard fed by the metric view.
  - Conceptual Power BI integration to Databricks.
- Discussion: mapping current Excel/Power BI flows to this new pattern.

### 3.2 Out of Scope

- Participant logins, cluster setup, or any live hands‑on.
- Deep Spark/cluster tuning, jobs, or pipelines.
- Detailed ML/MLflow content.
- Security/identity/DevOps deep dive (only high‑level governance).

***

## 4. Data Model & Setup (Pre‑Workshop)

Claude Code should generate code/templates; you will load/update data yourself.

### 4.1 Catalog/Schemas

Create:

- Catalog: `agldata`
- Schemas:
  - `trading` (curated NEM / trading tables)
  - `metrics` (metric views; schema can be `trading` if preferred)

Unity Catalog governs access, lineage, and documentation. [docs.databricks](https://docs.databricks.com/aws/en/getting-started/best-practices)

### 4.2 Curated NEM Table

**Name**

- `agldata.trading.curated_nem_prices`

**Purpose**

- Clean, analytics‑friendly NEM‑style dataset used consistently across all demos (SQL, Genie, Assistant, dashboards, Power BI). [aemo.com](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/data-dashboard-nem)

**Columns**

| Column          | Type       | Description                                                      |
|-----------------|-----------|------------------------------------------------------------------|
| `interval_start`| TIMESTAMP | Start of NEM interval (dispatch/trading). [aemo.com](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/data-dashboard-nem)              |
| `date`          | DATE      | Calendar date (derived from `interval_start`).                   |
| `region_id`     | STRING    | NEM region (NSW1, QLD1, VIC1, SA1, TAS1). [aemo.com](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/data-dashboard-nem)              |
| `rrp`           | DOUBLE    | Regional Reference Price ($/MWh). [aemo.com](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/data-dashboard-nem)                       |
| `demand_mw`     | DOUBLE    | Scheduled demand (MW). [aemo.com](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/data-dashboard-nem)                                  |
| `is_peak`       | BOOLEAN   | True if interval is in “peak” (e.g., 17:00–21:00).              |

**Data**

- Use real NEMWeb data or a realistic synthetic subset covering at least 7–30 days and multiple regions. [aemo.com](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-data-nemweb)

**Metadata**

- Add column comments and clear names in Unity Catalog (so Genie and Assistant have context). [docs.databricks](https://docs.databricks.com/aws/en/genie/set-up)

### 4.3 Metric View

**Name**

- `agldata.trading.mv_nem_price_metrics`

**Purpose**

- Provide reusable measures for NEM analytics (avg price, volatility, demand) over standard dimensions for SQL, Genie, dashboards, and BI. [docs.databricks](https://docs.databricks.com/aws/en/metric-views/)

**Measures**

- `avg_price` = average `rrp`
- `price_volatility` = stddev `rrp`
- `peak_price` = max `rrp`
- `avg_demand_mw` = average `demand_mw`
- `peak_demand_mw` = max `demand_mw`

**Dimensions**

- `region_id`
- `date`
- Optionally derived: `hour_of_day`, `day_of_week`

Metric view must be implemented as a proper Unity Catalog metric view. [community.databricks](https://community.databricks.com/t5/technical-blog/how-to-deploy-metric-views-with-dabs/ba-p/138432)

### 4.4 Genie Space

**Name**

- “AGL Trading Analytics – NEM”

**Datasets attached**

- `agldata.trading.curated_nem_prices`
- `agldata.trading.mv_nem_price_metrics` (if supported) [learn.microsoft](https://learn.microsoft.com/en-us/azure/databricks/metric-views/)

**Seeded example questions**: [databricks](https://www.databricks.com/product/business-intelligence/ai-bi-genie)

- “What was the average spot price in NSW over the last 7 days?”
- “How does that compare to QLD over the same period?”
- “Show a line chart of daily average price by region for the last 30 days.”
- “When were the top 5 demand peaks in VIC this week, and what were the prices at those times?”

Configure roles:

- Authors: facilitator + nominated AGL data lead.
- Users: trading/risk personas.

### 4.5 Databricks Assistant

Requirements: [docs.databricks](https://docs.databricks.com/aws/en/notebooks/databricks-assistant-faq)

- Assistant enabled in:
  - SQL Editor.
  - Notebooks.
- Workspace permissions configured so you can use Assistant with the above tables.

### 4.6 Optional Power BI Setup

- Databricks SQL endpoint/warehouse created.
- Power BI Desktop configured with Databricks connector:
  - Host, HTTP path, token.
- A simple PBIX connecting to `mv_nem_price_metrics` and showing 1–2 visuals. [docs.databricks](https://docs.databricks.com/aws/en/partners/bi/power-bi)

***

## 5. Detailed Agenda & Script

### 5.1 0–5 min – Context: where you are, where Databricks fits

**Goal:** Make them comfortable; relate to Excel/Power BI reality.

Key points:

- “You mostly live in Excel and Power BI today; that’s expected.”  
- AGL is driving more data & AI capabilities and cloud‑based analytics. [itnews.com](https://www.itnews.com.au/news/agl-energy-is-becoming-more-data-driven-with-its-workforce-planning-612067)
- Databricks is:
  - The place where NEM and trading data is ingested, cleaned, and governed (Delta + Unity Catalog). [learn.microsoft](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/)
  - Where AI tools (Genie, Assistant) help you ask questions and write queries. [docs.databricks](https://docs.databricks.com/aws/en/notebooks/code-assistant)
  - A backend that can feed Power BI, not a replacement for it. [databricks](https://www.databricks.com/blog/enabling-business-users-databricks)

Outcome: mental model = “Databricks behind the scenes; Excel/Power BI + AI at the front.”

***

### 5.2 5–12 min – Curated NEM data: single source of truth

**Goal:** Show a familiar dataset in Databricks, framed as “the table you wish you had in Power BI/Excel.”

Demo:

1. In Catalog Explorer, open `agldata.trading.curated_nem_prices`. [datacamp](https://www.datacamp.com/tutorial/databricks-unity-catalog-guide)
2. Show schema and sample rows; highlight:
   - `interval_start`, `date`, `region_id`, `rrp`, `demand_mw`, `is_peak`. [aemo.com](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-management-system-mms-data)
3. Explain this is cleaned NEMWeb‑style data they already use in some form, but now:
   - Governed.
   - Documented.
   - Reusable by many tools. [datacamp](https://www.datacamp.com/tutorial/delta-lake)
4. Run a quick SQL summary:

   ```sql
   SELECT
     region_id,
     COUNT(*) AS rows,
     AVG(rrp) AS avg_price,
     AVG(demand_mw) AS avg_demand
   FROM agldata.trading.curated_nem_prices
   WHERE date >= current_date() - INTERVAL 7 DAYS
   GROUP BY region_id;
   ```

Message: “This replaces multiple CSV exports and one‑off extracts you currently juggle.”

***

### 5.3 12–25 min – AI/BI Genie: ask NEM questions in English

**Goal:** Deliver the main “wow” moment with minimal friction.

Demo:

1. Open Genie → “AGL Trading Analytics – NEM”. [docs.databricks](https://docs.databricks.com/aws/en/genie/)
2. Explain:
   - Genie is AI‑powered BI: ask questions in natural language over governed data. [databricks](https://www.databricks.com/blog/aibi-genie-now-generally-available)
   - It uses your metadata and examples to generate SQL and visuals.
3. Ask scripted questions, e.g.:
   - “What was the average spot price in NSW over the last 7 days?”
   - “How does that compare to QLD over the same period?”
   - “Show me a line chart of daily average price by region for the last 30 days.”
   - “When were the top 5 demand peaks in NSW this week, and what were the prices at those times?”
4. For one question:
   - Click through to show generated SQL and confirm it hits the curated table/metric view. [learn.microsoft](https://learn.microsoft.com/en-us/azure/databricks/genie/)
5. Invite 1–2 questions from the room (e.g. “Which region had the highest volatility last month?”) and refine phrasing as needed.

Key framing:

- “This is like asking Power BI a question without building a new report.”
- “Trading Analytics can curate which data Genie sees, so it stays safe and meaningful.”

***

### 5.4 25–35 min – Databricks Assistant for SQL (copilot, not class)

**Goal:** Show how analysts can get SQL written for them, then tweak.

Demo (SQL Editor):

1. Open SQL Editor; ensure `agldata` is selected.
2. Use Assistant with prompt:

   > “Write a SQL query over `agldata.trading.curated_nem_prices` that, for the last 7 days, returns average price, price volatility, and peak demand by region, ordered by average price descending.”

3. Review SQL, run, show table of results. [learn.microsoft](https://learn.microsoft.com/en-us/azure/databricks/notebooks/databricks-assistant-faq)
4. Refinement prompt:

   > “Modify this query so it only includes NSW1 and QLD1 and shows daily values instead of overall 7‑day aggregates.”

5. Show the updated SQL and result.

Key messages:

- They don’t have to remember every SQL detail; they can describe what they want, then adjust.
- SQL from Assistant can feed:
  - New metric views (later).
  - Downstream dashboards or Power BI models.

Notebook use is optional; keep this section to SQL only for time and simplicity.

***

### 5.5 35–45 min – Metric views + simple dashboard

**Goal:** Introduce the idea of a semantic layer and show a small dashboard.

#### Metric views

1. Describe metric views in 1–2 sentences:
   - “Metric views define measures (like avg price, volatility, peak demand) and dimensions once, so everyone reuses the same definitions in SQL, dashboards, Genie, and Power BI.” [advancinganalytics.co](https://www.advancinganalytics.co.uk/blog/simplifying-analytics-with-metric-views-in-databricks)
2. Show `agldata.trading.mv_nem_price_metrics` in Catalog.
3. Run example:

   ```sql
   SELECT
     region_id,
     date,
     avg_price,
     price_volatility,
     peak_demand_mw
   FROM agldata.trading.mv_nem_price_metrics
   WHERE date >= current_date() - INTERVAL 7 DAYS
   ORDER BY avg_price DESC;
   ```

Explain: “This is the same logic you might otherwise re‑implement in DAX/Excel formulas, but now centralised.”

#### Dashboard

1. From this query, create or open a simple dashboard:
   - Line chart: daily `avg_price` by region.
   - Bar chart: `price_volatility` by region.
   - KPI: `peak_price` over last 24h. [databricks](https://www.databricks.com/resources/demos/videos/unlocking-power-databricks-aibi)
2. Show the dashboard page; you don’t need to build it entirely live—just add or tweak one tile.

Message: “Dashboards here share the same semantics as Genie and SQL; Power BI can sit on top of this too.”

***

### 5.6 45–55 min – Map current Excel/Power BI workflows to Databricks

**Goal:** Anchor everything in how they actually work today.

Discussion prompts:

- “What are 2–3 recurring Excel or Power BI reports you run with NEM data?”
- For each, sketch verbally how it would look with Databricks:
  - Source: curated NEM table instead of CSV.
  - Logic: metric view defines the KPI (avg price, volatility). [docs.databricks](https://docs.databricks.com/aws/en/metric-views/)
  - Consumption:
    - Power BI connects directly to Databricks (DirectQuery or import). [beyondkey](https://www.beyondkey.com/blog/databricks-integration-with-power-bi/)
    - Genie for ad‑hoc questions beyond the report.
    - Assistant to help create new SQL for future reports.

Optional (if time):

- Show a pre‑built Power BI report connecting to the Databricks SQL endpoint and using `mv_nem_price_metrics`:
  - Show the connection settings briefly.
  - Emphasise “live governed data, not files.”

***

### 5.7 55–60 min – Wrap‑up & next steps

Summarise:

1. Databricks as governed, AI‑powered data layer for NEM & trading analytics, not a replacement for Excel/Power BI. [docs.databricks](https://docs.databricks.com/aws/en/lakehouse-architecture/)
2. Genie for natural‑language questions; Assistant for SQL help. [databricks](https://www.databricks.com/product/business-intelligence/ai-bi-genie)
3. Metric views as the single source of KPI definitions across tools. [community.databricks](https://community.databricks.com/t5/technical-blog/how-to-deploy-metric-views-with-dabs/ba-p/138432)

Propose next steps:

- Working session (60–90 min) with Trading Analytics leads to:
  - Choose 2–3 tables to curate (NEM + internal).
  - Design first 1–2 production metric views.
  - Write 10–15 canonical Genie questions for a production space. [sunnydata](https://www.sunnydata.ai/blog/databricks-ai-bi-genie)
- Plan a **hands‑on** lab:
  - Either “Genie for Trading Analytics” (everyone in Genie).
  - Or “Power BI + Databricks” (build a report on the metric view).

***

## 6. Non‑Functional Requirements

- **Time discipline:** each segment rehearsed; no more than 5 minutes over per block.
- **Reliability:** all objects (tables, metric views, Genie space, dashboards) tested before session.
- **Data sensitivity:** NEMWeb or anonymised data only; no confidential positions. [visualisations.aemo.com](http://visualisations.aemo.com.au/aemo/nemweb/)
- **Simplicity:** keep notebooks minimal or skip if timing gets tight.

***

## 7. Artefacts for Claude Code to Generate

When you feed this PRD to Claude Code, ask it to generate:

1. **SQL DDL and example queries**
   - `CREATE TABLE agldata.trading.curated_nem_prices (...)`
   - Example analyses used in the workshop:
     - 7‑day regional summary.
     - Metric view query snippets.
2. **Metric view definition**
   - SQL (or appropriate config) for `agldata.trading.mv_nem_price_metrics` with measures/dimensions and comments. [learn.microsoft](https://learn.microsoft.com/en-us/azure/databricks/metric-views/)
3. **Notebook skeleton**
   - Optional: PySpark notebook with cells for:
     - Loading `curated_nem_prices`.
     - Example 95th percentile analysis (not strictly needed for the 60‑min version but useful later).
4. **Slide/markdown outline**
   - Section headings and bullet points matching the agenda.
5. **Genie configuration notes**
   - Table list, example questions, and description of the dataset (for manual setup in UI). [databricks](https://www.databricks.com/blog/aibi-genie-now-generally-available)
6. **Optional Power BI notes**
   - Steps to connect Power BI to Databricks SQL and use `mv_nem_price_metrics` as the model source. [cdata](https://www.cdata.com/kb/tech/databricks-powerbi-gateway.rst)
