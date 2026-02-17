# PRD: ML and data pipelines workshop series

## Overview

A series of 1-hour workshops for technical teams (Load Forecasting and Market Modelling) who currently manage their own Python-based modelling systems and data pipelines. Both teams are interested in ML capabilities and exploring whether Databricks could simplify or replace their existing infrastructure.

## Target audience

### Combined profile

| Attribute | Description |
|-----------|-------------|
| Technical level | High - build and maintain their own models and pipelines |
| Current tools | Python scripts, custom data pipelines, multiple data sources |
| Domain | Energy forecasting (load, generation, battery optimization, outages) |
| Databricks awareness | Low - need foundational understanding before deep dives |
| Primary interest | ML capabilities, pipeline management, system consolidation |
| Secondary interest | Monitoring (pipelines, models, operations) |

### Specific team contexts

**Load Forecasting Team**
- Pull data from multiple servers
- Build custom load forecasting models
- Manage custom data pipelines
- Actively considering Databricks migration

**Market Modelling Team**
- Run Python scripts for generation forecasts (from Plexus)
- Battery optimization and outage models
- Earlier stage - exploring options

## Workshop objectives

By the end of the series, participants should:

1. Understand how Databricks unifies data engineering, ML, and operations
2. See a clear path from "Python scripts on servers" to "managed platform"
3. Know the specific Databricks features that address their use cases
4. Have hands-on experience with relevant tools
5. Be able to articulate whether Databricks fits their needs

## Proposed workshop series

### Workshop 1: Platform foundations and data pipelines (60 min)

**Goal:** Demonstrate how Databricks replaces fragmented data infrastructure

| Section | Duration | Content |
|---------|----------|---------|
| Introduction | 10 min | Platform overview, Unity Catalog, Serverless compute |
| Data ingestion | 20 min | Lakeflow Declarative Pipelines (streaming + batch), custom data sources |
| Live demo | 20 min | Build a pipeline that ingests from external API, transforms, lands in Delta |
| Q&A | 10 min | Discussion: mapping their current pipelines to Databricks |

**Key messages:**
- Replace multiple servers with one platform
- Declarative pipelines vs custom scripts
- Built-in lineage, quality checks, monitoring
- Serverless = no cluster management

**Demo:**
- NEMWEB energy data pipeline ingesting DISPATCHREGIONSUM (demand) and DISPATCHPRICE (prices)
- Show how their "pull from multiple servers" pattern becomes a single declarative pipeline
- Demonstrate data quality expectations and failure handling

---

### Workshop 2: ML workflows and MLflow (60 min)

**Goal:** Show end-to-end ML lifecycle from experimentation to deployment

| Section | Duration | Content |
|---------|----------|---------|
| ML landscape | 10 min | Where does ML fit in Databricks? Feature Store, MLflow, Model Serving |
| Experiment tracking | 15 min | MLflow experiments, runs, parameters, metrics, artifacts |
| Model registry | 15 min | Versioning, staging, production promotion, lineage |
| Live demo | 15 min | Train a forecasting model, log to MLflow, register, deploy |
| Q&A | 5 min | Discussion: their current model management pain points |

**Key messages:**
- MLflow is open source - no lock-in
- Experiment tracking replaces ad-hoc notebooks/scripts
- Model Registry provides governance and audit trail
- Unity Catalog integration for model lineage

**Demo:**
- Load forecasting model using regional demand + weather features
- Show experiment tracking: hyperparameters, metrics (MAPE, RMSE), feature importance
- Compare multiple model runs (Prophet vs XGBoost vs LightGBM)
- Register best model to Unity Catalog with lineage to training data

---

### Workshop 3: Model serving and monitoring (60 min)

**Goal:** Show how models move from development to production with observability

| Section | Duration | Content |
|---------|----------|---------|
| Deployment options | 10 min | Batch inference, Model Serving endpoints, real-time vs batch |
| Model Serving | 15 min | Deploying endpoints, scaling, A/B testing |
| Inference tables | 15 min | Automatic logging of inputs/outputs for monitoring |
| Lakehouse Monitoring | 15 min | Drift detection, data quality, alerting |
| Q&A | 5 min | Discussion: their monitoring requirements |

**Key messages:**
- Production ML needs observability
- Inference tables capture everything automatically
- Drift detection catches model degradation early
- Alerts integrate with existing tools (email, Slack, PagerDuty)

**Demo:**
- Deploy load forecast model as real-time endpoint
- Show inference tables capturing predictions + actuals
- Lakehouse Monitoring dashboard showing prediction drift over time
- Alert configuration: "notify when MAPE exceeds threshold"

---

### Workshop 4: Advanced ML and GenAI (60 min) - Optional

**Goal:** Explore advanced capabilities for teams ready to go deeper

| Section | Duration | Content |
|---------|----------|---------|
| AutoML | 15 min | Automated model selection and hyperparameter tuning |
| Feature Store | 15 min | Centralized feature management, point-in-time lookups |
| GenAI overview | 15 min | Foundation Model APIs, RAG patterns, Mosaic AI |
| Demo | 10 min | AutoML on demand forecasting - show how it explores model space |
| Q&A | 5 min | Next steps and follow-up |

**Key messages:**
- AutoML accelerates experimentation
- Feature Store prevents training-serving skew
- GenAI capabilities available within same platform

---

## Workshop format

**Demo-focused approach selected**

- 70% demonstration, 30% discussion/Q&A
- Instructor drives, participants observe and ask questions
- Lower barrier to entry, can cover more ground in 60 minutes
- Demos use realistic energy data matching their domain

## Assumed data and use cases

Based on the teams working with Plexos and load forecasting systems, the demos will use data representative of their actual workflows.

### Load Forecasting Team - likely data sources

| Data type | Source | Use case |
|-----------|--------|----------|
| Historical demand | AEMO/NEMWEB (DISPATCHREGIONSUM) | Training load forecast models |
| Weather data | BOM / commercial providers | Temperature, humidity, solar irradiance as features |
| Calendar/events | Internal | Public holidays, major events, school terms |
| Historical prices | NEMWEB (DISPATCHPRICE) | Price-responsive demand modelling |
| Rooftop solar | AEMO APVI | Behind-the-meter generation adjustment |

### Market Modelling Team - likely data sources (Plexos inputs/outputs)

| Data type | Source | Use case |
|-----------|--------|----------|
| Generator data | Plexos / AEMO | Capacity, heat rates, fuel costs, availability |
| Bid/offer data | NEMWEB (BIDPEROFFER_D, BIDDAYOFFER_D) | Market behaviour analysis |
| Interconnector limits | AEMO | Transfer constraints between regions |
| Renewable forecasts | AEMO / ASEFS | Wind and solar generation profiles |
| Battery parameters | Internal / Plexos | State of charge, efficiency, cycling limits |
| Outage schedules | AEMO MT PASA | Planned maintenance, forced outages |
| Forward prices | ASX Energy | Contract pricing, hedging analysis |

### Demo data strategy

We'll use **NEMWEB data** (already available in the existing lab infrastructure) to demonstrate concepts. This is real AEMO data that both teams would recognize:

| Demo dataset | NEMWEB table | Maps to their use case |
|--------------|--------------|------------------------|
| Regional demand | DISPATCHREGIONSUM | Load forecasting target variable |
| Spot prices | DISPATCHPRICE | Price forecasting, market modelling |
| Unit generation | DISPATCH_UNIT_SCADA | Generator dispatch patterns |
| Generator bids | BIDPEROFFER_D | Market behaviour, Plexos validation |
| 5-min forecasts | P5MIN_REGIONSOLUTION | Short-term forecasting benchmarks |

**Weather data:** We'll either use public BOM data or synthetic weather features to demonstrate the forecasting workflow.

## Prerequisites

### For participants
- None required - just attend and ask questions

### For instructor
- Databricks workspace with NEMWEB data loaded
- Pre-built demo notebooks for each workshop
- Sample trained models ready to show (don't train live - too risky)

## Success metrics

| Metric | Target |
|--------|--------|
| Attendance | 80%+ of invited team members |
| Engagement | Active Q&A, relevant questions |
| Follow-up | Teams request deeper dive or POC |
| Clarity | Participants can articulate "Databricks can/cannot help us with X" |

## Open questions for customer

1. **Timeline:** When do they want to start? How spaced should workshops be?
2. **Specific pain points:** What's most broken in their current setup? (helps prioritize demo content)
3. **Decision timeline:** Are they actively evaluating migration, or exploratory?
4. **Plexos integration:** Do they want to see Plexos data ingestion specifically, or is generic energy data sufficient?
5. **Attendee count:** How many people per session? (affects Q&A dynamics)

## Recommended sequence

```
Week 1: Workshop 1 - Platform foundations and data pipelines
Week 2: Workshop 2 - ML workflows and MLflow
Week 3: Workshop 3 - Model serving and monitoring
Week 4: (Optional) Workshop 4 - Advanced ML and GenAI
        OR
        Deep-dive on specific topic based on interest
```

## Materials to prepare

| Item | Description | Status |
|------|-------------|--------|
| **Workshop 1 materials** | | |
| Slide deck | Platform overview, Lakeflow concepts | Not started |
| Demo notebook: Data ingestion | NEMWEB pipeline with DISPATCHREGIONSUM + DISPATCHPRICE | Not started |
| Demo notebook: Pipeline monitoring | Show lineage, data quality, run history | Not started |
| **Workshop 2 materials** | | |
| Slide deck | MLflow concepts, experiment tracking | Not started |
| Demo notebook: Model training | Load forecast with weather features, multiple algorithms | Not started |
| Pre-trained models | Prophet, XGBoost, LightGBM already logged to MLflow | Not started |
| Demo notebook: Model registry | Show versioning, promotion, lineage | Not started |
| **Workshop 3 materials** | | |
| Slide deck | Model serving, inference tables, monitoring | Not started |
| Demo notebook: Deployment | Deploy forecast model to serving endpoint | Not started |
| Demo notebook: Monitoring | Lakehouse Monitoring dashboard, drift detection | Not started |
| Sample inference data | Historical predictions + actuals for monitoring demo | Not started |
| **Workshop 4 materials** | | |
| Slide deck | AutoML, Feature Store, GenAI overview | Not started |
| Demo notebook: AutoML | Demand forecasting with AutoML | Not started |
| Demo notebook: Feature Store | Time-series features with point-in-time lookups | Not started |
| **Supporting materials** | | |
| NEMWEB data (existing) | Already loaded via nemweb-lab infrastructure | Available |
| Weather data | BOM historical or synthetic temperature/humidity | Not started |
| Backup recordings | Pre-recorded demos in case of live issues | Not started |

## Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Too high-level for technical audience | Focus on "how" not just "what" - show code, configs |
| Too deep for 60 minutes | Strict time-boxing, parking lot for deep questions |
| No sample data available | Use NEMWEB or public energy datasets |
| Participants have varied skill levels | Pitch to middle, offer follow-up for advanced questions |
| Teams decide Databricks isn't a fit | That's a valid outcome - better to know early |

## Appendix A: Current state vs Databricks mapping

This table helps frame the demos in terms of what they're doing today vs what Databricks provides.

### Load Forecasting Team

| Current state | Pain points | Databricks equivalent |
|---------------|-------------|----------------------|
| Pull data from multiple servers via scripts | Fragile, no retry logic, manual scheduling | Lakeflow pipelines with built-in orchestration |
| Custom Python ETL pipelines | Hard to monitor, no lineage, debugging is painful | Declarative pipelines with data quality expectations |
| Models in Jupyter notebooks | No versioning, hard to compare experiments | MLflow experiment tracking |
| Model files saved to shared drive | No governance, unclear which version is "production" | Unity Catalog Model Registry |
| Cron jobs for batch predictions | No monitoring, silent failures | Databricks Jobs with alerting |
| Manual model retraining | Drift goes unnoticed until stakeholders complain | Lakehouse Monitoring with drift alerts |

### Market Modelling Team

| Current state | Pain points | Databricks equivalent |
|---------------|-------------|----------------------|
| Python scripts for generation forecasts | Ad-hoc, hard to reproduce | Notebooks + MLflow for reproducibility |
| Plexos model runs | Long-running, manual data prep | Lakeflow for automated data prep, Workflows for orchestration |
| Battery optimization models | Custom code, no experiment tracking | MLflow + hyperparameter tuning |
| Outage model outputs | Scattered across files | Delta Lake with time travel, unified storage |
| Results shared via Excel/email | Version confusion, no single source of truth | Unity Catalog tables + dashboards |

### Common transformation

```
Before:                                    After:
┌─────────────┐                           ┌─────────────────────────────────────┐
│ Server A    │──┐                        │         Databricks Lakehouse        │
├─────────────┤  │                        │  ┌─────────────────────────────────┐│
│ Server B    │──┼── Python ── Model ──►  │  │ Lakeflow    │ MLflow  │ Serving ││
├─────────────┤  │   scripts   files      │  │ Pipelines   │ Registry│ Endpoint││
│ Server C    │──┘                        │  └─────────────────────────────────┘│
└─────────────┘                           │         Unity Catalog governance    │
                                          └─────────────────────────────────────┘
```

## Appendix B: Relevant Databricks features

### Data engineering
- Lakeflow Declarative Pipelines (streaming tables, materialized views)
- Delta Lake (ACID, time travel, schema evolution)
- Unity Catalog (governance, lineage, access control)
- Serverless compute (no cluster management)

### ML/MLOps
- MLflow (experiments, tracking, registry)
- Feature Store (centralized features, point-in-time)
- Model Serving (real-time endpoints, scaling)
- AutoML (automated model selection)

### Monitoring
- Lakehouse Monitoring (drift, quality, profiling)
- Inference tables (automatic input/output logging)
- System tables (audit, billing, usage)
- Alerting (email, Slack, webhooks)

### GenAI (if relevant)
- Foundation Model APIs (hosted LLMs)
- Vector Search (embeddings, RAG)
- AI Functions (SQL-based AI)
- Mosaic AI (agent framework)
