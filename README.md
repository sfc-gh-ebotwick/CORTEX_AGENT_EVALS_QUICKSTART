# Cortex Agent Evaluations Quickstart

A practical toolkit for building and testing evaluation datasets with Snowflake's Cortex Agent Evaluations (Private Preview).

## What's included

- **Evaluation Dataset Builder**: Streamlit app for creating agent evaluation datasets
- **Setup Scripts**: SQL to configure sample agents and tables

## Setup

### 1. Run the setup script

```sql
-- In Snowsight or your SQL client
-- Creates sample agents and evaluation tables
@setup.sql
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### 4. Launch the app

```bash
cd AGENT_EVAL_ASSISTANT
streamlit run agent_observability_app.py
```

## Using the app

The app helps you build evaluation datasets by:

1. **Loading data** from agent logs or existing tables
2. **Adding records** manually for edge cases
3. **Editing** to refine queries and expected outputs
4. **Exporting** to Snowflake tables

Datasets follow this schema:
```sql
INPUT_QUERY VARCHAR
EXPECTED_TOOLS VARIANT  -- {ground_truth_invocations: [...], ground_truth_output: "..."}
```

## Requirements

- Snowflake account with Cortex Agent Evaluations enabled (Private Preview)
- Python 3.8+
- Access to agent observability events

## Docs

Cortex Agent Evaluations: https://docs.snowflake.com/LIMITEDACCESS/cortex-agent-evaluations
