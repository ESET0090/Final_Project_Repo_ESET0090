# Load Forecast MLOps Project (Starter)

## Requirements
- Docker & Docker Compose
- Python 3.10 (for local scripts)
- Git

## Quickstart (local)
1. Copy config/project_config.yaml or edit for your environment.
2. Start infra:
   ```bash
   cd infrastructure
   docker-compose up --build
Wait for services:

Airflow UI: http://localhost:8080 (user: admin / admin)

MLflow UI: http://localhost:5000

Model API: http://localhost:8000/docs

Ingest sample data:

bash
Copy code
python src/data/ingestion.py data/raw/sample_meter_data.csv
python src/data/features.py
python src/models/train.py
Check MLflow UI for runs. Promote model to Production via MLflow UI (or use mlflow client).

Start model API (if not started via compose):