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