# NEMWEB ML Workshop Facilitator Playbook

This playbook is the operational runbook for a 1-hour session with two tracks
in one flow.

## 1) Pre-Flight Checklist

Run from `databricks-nemweb-ml-lab/`:

```bash
databricks bundle validate --target dev --var="environment=dev" --profile daveok
databricks bundle deploy --target dev --var="environment=dev" --profile daveok
```

Confirm:

- Bundle deploy completed with no errors
- Warehouse ID configured (`warehouse_id` variable)
- Workshop jobs and prediction pipeline job are present
- App resource `nem_forecast` is present

## 2) Verified Run Order

Execute in this order:

```bash
databricks bundle run streaming_pipeline_job --target dev --var="environment=dev" --profile daveok --no-wait
databricks bundle run load_forecast_workshop --target dev --var="environment=dev" --profile daveok
databricks bundle run market_modelling_workshop --target dev --var="environment=dev" --profile daveok
databricks bundle run prediction_pipeline_job --target dev --var="environment=dev" --profile daveok
databricks bundle run nem_forecast --target dev --var="environment=dev" --profile daveok --no-wait
```

Expected state:

- Both workshop jobs terminate `SUCCESS`
- Prediction pipeline job terminates `SUCCESS`
- App starts and reports running URL

## 3) Timeboxed Live Demo Checkpoints

- 0:00-0:05
  - Show architecture and run order
  - Confirm data source status (`streaming_pipeline_job` triggered/running)
- 0:05-0:30 (Track A)
  - Execute and validate load forecasting notebook as job
  - Show model registration + champion alias
  - Show `demand_predictions_workshop` rows
- 0:30-0:48 (Track B)
  - Execute and validate market modelling notebook as job
  - Show market stress function + metrics
  - Show `price_predictions_workshop` rows
- 0:48-0:55
  - Run prediction pipeline refresh
  - Confirm app-facing `demand_predictions` and `price_predictions` are fresh
- 0:55-1:00
  - Open app and review forecast quality with audience

## 4) Failure and Recovery Matrix

- Symptom: `prediction_pipeline_job` fails with "MANAGED table already exists"
  - Cause: Legacy non-pipeline managed tables exist for `demand_predictions` or
    `price_predictions`
  - Recovery:
    ```bash
    databricks api post /api/2.0/sql/statements --profile daveok --json '{"statement":"DROP TABLE IF EXISTS daveok.ml_workshops.demand_predictions","warehouse_id":"e4082fdb7ea19a15"}'
    databricks api post /api/2.0/sql/statements --profile daveok --json '{"statement":"DROP TABLE IF EXISTS daveok.ml_workshops.price_predictions","warehouse_id":"e4082fdb7ea19a15"}'
    databricks bundle run prediction_pipeline_job --target dev --var="environment=dev" --profile daveok
    ```

- Symptom: Workshop run fails at MLflow experiment creation
  - Cause: Shared path permissions or invalid experiment path
  - Recovery: rerun with a writable `experiment_path` (for example
    `/Users/<user>/nemweb/...`)

- Symptom: Pipeline update conflict ("already in progress")
  - Cause: Concurrent update against same pipeline
  - Recovery: wait for current update to complete, then rerun job

- Symptom: App starts but shows demo mode only
  - Cause: missing warehouse/env auth context or missing prediction tables
  - Recovery: confirm `prediction_pipeline_job` succeeded and app env vars are set
    from bundle resource

## 5) Fallback Paths (If Time or Infra Constrains)

- If workshop execution is slow:
  - Show last successful run URL and key outputs
  - Continue with conceptual walkthrough and metrics interpretation

- If app startup is delayed:
  - Continue with SQL table inspection in UC and discuss dashboard logic
  - Re-open app at end of session

- If one track fails:
  - Complete the other track live
  - Use saved run artifacts to narrate the failed track

## 6) Verification Evidence Capture

Capture these items for post-session notes:

- Run URLs for:
  - `streaming_pipeline_job`
  - `load_forecast_workshop`
  - `market_modelling_workshop`
  - `prediction_pipeline_job`
  - `nem_forecast` app URL
- Job terminal states (`SUCCESS`/`FAILED`)
- Confirmation that both workshop runs had no exported `state=error` cells

## 7) Facilitator Quick Script

Use this short script in-session:

- "We train in workshop notebooks, register champion models in UC, then refresh
  pipeline-managed prediction tables consumed by the app."
- "Workshop outputs are isolated (`*_workshop`) to avoid colliding with app
  production tables."
- "This keeps demos stable and production wiring realistic."
