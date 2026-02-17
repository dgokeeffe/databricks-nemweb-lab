# ML Workshops

## Target audience

Technical teams who:
- Build and maintain forecasting systems (load, generation, optimization)
- Run Python scripts and manage data pipelines
- Pull data from multiple servers
- Are evaluating platform consolidation

## Workshop approach

- **Hands-on over slides** - participants run real code
- **Their code, unchanged** - show existing Python works without rewrites
- **Start with pain points** - ask team leads what's annoying today
- **Practical, not sales-y** - show what's real, acknowledge limitations

## Main workshop (recommended)

| File | Duration | Format |
|------|----------|--------|
| `01_hands_on_ml_workshop.py` | 90 min | Hands-on |

**Agenda:**
1. Pain point discovery (10 min) - Discussion with team leads
2. Your code, unchanged (15 min) - Run existing Python scripts
3. Add MLflow tracking (25 min) - 3 lines to track experiments
4. Turn into scheduled pipeline (20 min) - Workflows with alerting
5. What else is possible (10 min) - Demo of registry, serving, monitoring
6. Q&A (10 min)

## Deep-dive workshops (optional)

For teams wanting more depth on specific topics:

| File | Topic | Duration |
|------|-------|----------|
| `01_platform_and_pipelines.py` | Lakeflow pipelines, data quality | 60 min |
| `02_ml_workflows_mlflow.py` | MLflow tracking, model registry | 60 min |
| `03_serving_and_monitoring.py` | Model endpoints, drift detection | 60 min |
| `04_advanced_ml_genai.py` | AutoML, Feature Store, GenAI | 60 min |

## Setup

The `00_workshop_data_setup.py` notebook creates the schema and volume.
Data is loaded by the MMS pipeline which runs after setup.

**Prerequisites:**
- Databricks workspace with Unity Catalog
- `nemweb_datasource` wheel installed (via job environment)
- Serverless compute or DBR 15.4+

## Running the workshop

```bash
# Deploy via bundle
databricks bundle deploy --var="environment=dev"

# Run main workshop (standalone - doesn't need data setup)
# Just open 01_hands_on_ml_workshop.py in workspace

# Run full workshop series with data
databricks bundle run ml_workshops --target dev
```

## Facilitation tips

1. **Start with questions, not slides**
   - "What's the most annoying part of your current workflow?"
   - Write answers on whiteboard, reference throughout

2. **Let them drive**
   - If someone wants to try something, let them
   - Tangents based on their questions are good

3. **Acknowledge limitations**
   - "This won't solve X" is more credible than overselling
   - Be honest about learning curve

4. **End with concrete next step**
   - "What would you try first?"
   - Offer to help them run one of their scripts
