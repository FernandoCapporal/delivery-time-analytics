# delivery-time-analytics

End-to-end data pipeline and analytics solution for delivery time monitoring and prediction.
Covers weekly ETL orchestration with Apache Airflow, an interactive Streamlit dashboard,
and an optional predictive model for Actual Time of Delivery (ATD).

---

## Project Overview

This project automates the extraction, transformation, and visualization of weekly delivery
data. It is structured around three core components:

1. **Data Pipeline (Airflow)** — A DAG that runs weekly, dynamically computes the previous
   week's date range using `{{ ds }}`, executes a SQL query against the source tables, and
   writes the output to the `AA_tables` schema.

2. **Streamlit Dashboard** — An interactive web app that surfaces key delivery metrics,
   operational insights, and (optionally) ATD predictions for stakeholders.

3. **Predictive Model (Bonus)** — A machine learning model trained on the prepared dataset
   to forecast Actual Time of Delivery (ATD), improving delivery accuracy and operational
   planning.

---

## Repository Structure
delivery-time-analytics/
│
├── dags/                        # Airflow DAGs
│   └── weekly_delivery_etl.py   # Main DAG: extracts and loads weekly delivery data
│
├── notebooks/                   # Exploratory and modeling notebooks
│   ├── 01_eda.ipynb             # Exploratory Data Analysis
│   ├── 02_feature_engineering.ipynb
│   └── 03_atd_model.ipynb       # ATD predictive model (bonus)
│
├── streamlit_app/               # Dashboard application
│   ├── app.py                   # Main Streamlit entry point
│   ├── components/              # Reusable UI components
│   └── utils.py                 # Data loading and helper functions
│
├── sql/                         # SQL query templates
│   └── weekly_delivery_query.sql
│
├── models/                      # Serialized trained models (e.g., .pkl, .joblib)
│
├── docker-compose.yml           # Postgres + pgAdmin + Airflow stack
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment variable template
└── README.md

---

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Database | PostgreSQL 16 |
| DB GUI | pgAdmin 4 |
| Dashboard | Streamlit |
| Modeling | scikit-learn / XGBoost |
| Notebooks | Jupyter |
| Containerization | Docker + Docker Compose |

---

## Getting Started

### Prerequisites

- Docker and Docker Compose installed
- Python 3.10+

### 1. Clone the repository
```bash
git clone https://github.com/your-username/delivery-time-analytics.git
cd delivery-time-analytics
```

### 2. Set up environment variables
```bash
cp .env.example .env
# Edit .env with your credentials
```

### 3. Start the infrastructure
```bash
mkdir -p dags logs plugins
echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose up -d
```

| Service | URL | Default credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| pgAdmin | http://localhost:5050 | admin@admin.com / admin |
| PostgreSQL | localhost:5432 | airflow / airflow |

### 4. Run the Streamlit dashboard
```bash
pip install -r requirements.txt
streamlit run streamlit_app/app.py
```

---

## Pipeline Details

The Airflow DAG `weekly_delivery_etl` runs every Monday and:

1. Computes the date range for the **previous calendar week** using `{{ ds }}`
2. Executes the parameterized SQL query against the source tables
3. Writes the result to `AA_tables.weekly_delivery_output` in PostgreSQL

The SQL template lives in `sql/weekly_delivery_query.sql` and is rendered at runtime
by the DAG using Airflow's templating engine.

---

## Dashboard

The Streamlit app connects to the PostgreSQL database and provides:

- Weekly delivery volume and trends
- On-time delivery rate by region / category
- ATD distribution and outlier detection
- (Bonus) Predicted vs actual delivery times

---

## Predictive Model (Bonus)

See `notebooks/03_atd_model.ipynb` for full walkthrough. The trained model is
serialized to `models/atd_model.joblib` and loaded by the Streamlit app for
real-time inference.

**Model approach:** Gradient Boosting (XGBoost) with cross-validated hyperparameter
tuning. Target variable: `actual_time_of_delivery`. Evaluation metric: MAE (minutes).

---

## Environment Variables

| Variable | Description |
|---|---|
| `POSTGRES_USER` | PostgreSQL username |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `POSTGRES_DB` | Target database name |
| `AIRFLOW_UID` | Host user UID for Airflow volumes |
| `DB_CONN_STRING` | SQLAlchemy connection string for Streamlit |

---

## Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Commit your changes: `git commit -m "feat: describe your change"`
3. Open a pull request against `main`

---

## License

MIT