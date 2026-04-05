# Delivery Time Analytics

End-to-end data pipeline and analytics solution for delivery time monitoring and prediction. Covers weekly ETL orchestration with Apache Airflow, machine learning model training, and an interactive Streamlit dashboard for stakeholder reporting.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Overview](#2-architecture-overview)
3. [Data Extraction](#3-data-extraction)
4. [ETL Workflow Design](#4-etl-workflow-design)
5. [Data Cleaning & Preprocessing](#5-data-cleaning--preprocessing)
6. [Feature Engineering](#6-feature-engineering)
7. [Model Development](#7-model-development)
8. [Evaluation Metrics](#8-evaluation-metrics)
9. [Dashboard](#9-dashboard)
10. [How to Run the Project](#10-how-to-run-the-project)
11. [Reproducibility](#11-reproducibility)
12. [Notes & Assumptions](#12-notes--assumptions)

---

## 1. Project Overview

### Business Context

In last-mile delivery, **Actual Time of Delivery (ATD)** is the elapsed time between a restaurant accepting an order and the order reaching the customer. It is one of the most critical KPIs in delivery operations: customers use it to evaluate reliability, operations teams use it to identify bottlenecks, and product teams use it to prioritize platform improvements.

Small improvements in ATD prediction accuracy translate directly into better ETA displays, improved courier dispatch decisions, and higher customer satisfaction scores.

### Problem Statement

Without a structured, automated pipeline:

- Delivery performance data is siloed across multiple source tables and requires manual extraction.
- Stakeholders have no single, up-to-date view of ATD trends, territory-level differences, or time-of-day patterns.
- Dispatch and operations teams cannot programmatically forecast delivery times at the trip level.

### Objectives

This project addresses those gaps through three components:

| Component | Goal |
|---|---|
| **ETL Pipeline** | Automate weekly extraction, transformation, and storage of delivery data from source tables into a queryable historical table |
| **Dashboard** | Provide stakeholders with an interactive, filtered view of ATD distributions, trends, and model performance |
| **Predictive Model** | Train and evaluate ML models that forecast ATD at the individual trip level, enabling real-time inference |

---

## 2. Architecture Overview

### End-to-End Workflow

```
Source Tables (PostgreSQL)
        │
        ▼
┌───────────────────────────────────────────┐
│  weekly_delivery_etl  (Airflow DAG)        │
│  Monday 00:00 UTC                          │
│                                            │
│  1. create_schema                          │
│  2. {country}_weekly_delivery_data  ──────►  uber_prod.{country}_delivery_trips_info
│  3. {country}_historic_delivery_data ────►  uber_prod.{country}_delivery_trips_historic_info
└───────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────┐
│  atd_model_training  (Airflow DAG)         │
│  Monday 02:00 UTC                          │
│                                            │
│  1. load_and_clean_data                    │
│  2. build_pre_assign_features  ──────────► X/y splits + preprocessing pipeline (.pkl)
│     build_post_assign_features ──────────► X/y splits + preprocessing pipeline (.pkl)
│  3. train_pre_assign_model ──────────────► best_model_pre_assign.pkl
│     train_post_assign_model ─────────────► best_model_post_assign.pkl
│  4. upload_artifacts_to_s3                 │
└───────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────┐
│  Streamlit Dashboard  (always-on)          │
│                                            │
│  • Overview KPIs                           │
│  • EDA: distributions, relationships,      │
│    time patterns                           │
│  • Performance: model error analysis       │
│  • Predictions: interactive inference form │
└───────────────────────────────────────────┘
```

### Step-by-Step Pipeline Explanation

1. **SQL Extraction** — Every Monday, Airflow renders a parameterized SQL query with the previous week's date window (`{{ ds }}`). The query joins four source tables, computes distance in kilometers, and calculates ATD in minutes.

2. **Weekly Snapshot** — Query output is written to `uber_prod.{country}_delivery_trips_info`, overwriting the previous weekly snapshot.

3. **Historical Accumulation** — A second SQL step merges the weekly snapshot into `uber_prod.{country}_delivery_trips_historic_info`, which accumulates all weekly batches. Idempotent: re-running the same batch date updates existing rows rather than inserting duplicates.

4. **Data Cleaning** — A downstream Airflow DAG reads the full historical table, applies multi-step cleaning (deduplication, outlier removal, timestamp validation, imputation), and persists a cleaned CSV.

5. **Feature Engineering** — Two variants of feature sets are built: pre-assignment (features available before a courier is dispatched) and post-assignment (adds dispatch delay, available after a courier accepts). Each variant produces a serialized preprocessing pipeline and stratified train/test splits.

6. **Model Training** — Four regression models are trained per variant, evaluated on the held-out test set, and the best model (lowest RMSE) is serialized to disk and uploaded to S3.

7. **Dashboard** — The Streamlit app loads the cleaned CSV and model artifacts, exposing interactive filtering, visualizations, model diagnostics, and single-trip inference.

### Repository Structure

```
delivery-time-analytics/
│
├── dags/
│   ├── weekly_delivery_etl/
│   │   ├── weekly_delivery_etl.py          # ETL DAG
│   │   └── queries/
│   │       ├── weekly_delivery_query.sql   # Weekly extraction query
│   │       └── historic_delivery_query.sql # Historical merge query
│   └── atd_model_training/
│       └── atd_model_training.py           # ML training DAG
│
├── dashboard/
│   ├── app.py                              # Streamlit entry point
│   ├── data.py                             # Data loading, filtering, KPIs
│   ├── models.py                           # Model loading and inference
│   └── charts.py                           # Plotly chart functions
│
├── db/
│   ├── init.sql                            # Schema and table definitions
│   ├── mock_init.ipynb                     # Notebook for seeding mock data
│   └── query_builder.sql                   # Helper queries
│
├── notebooks/
│   ├── 01_eda.ipynb                        # Exploratory Data Analysis
│   ├── 02_feature_engineering.ipynb        # Feature engineering walkthrough
│   └── 03_atd_model.ipynb                  # Model training and evaluation
│
├── docker-compose.yml                      # Full infrastructure stack
├── requirements.txt                        # Python dependencies
├── .env.example                            # Environment variable template
└── README.md
```

### Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Database | PostgreSQL 16 |
| Database GUI | pgAdmin 4 |
| Dashboard | Streamlit 1.45.1 |
| Data Processing | pandas 2.3.3 |
| ML Modeling | scikit-learn, XGBoost 3.2.0 |
| Visualization | Plotly 6.1.1 |
| Containerization | Docker + Docker Compose |
| Notebooks | Jupyter / IPython |

---

## 3. Data Extraction

### SQL Logic (`weekly_delivery_query.sql`)

The extraction query produces one row per completed delivery trip for the previous calendar week.

#### Tables Used

| Table | Purpose |
|---|---|
| `tmp.lea_trips_scope_atd_consolidation_v2` | Core trip data: timestamps, courier flow, trip identifiers |
| `delivery_matching.eats_dispatch_metrics_job_message` | Distance metrics: pickup and travel distance in meters |
| `kirby_external_data.cities_strategy_region` | Territory metadata: region and territory names |
| `dwh.dim_city` | City reference data: city name, country name |

#### Joins

```
lea_trips_scope_atd_consolidation_v2
  └── JOIN eats_dispatch_metrics_job_message ON delivery_trip_uuid = job_uuid
        └── JOIN dim_city ON edmjm.city_id = dim_city.city_id
              └── JOIN cities_strategy_region ON dim_city.city_id = csr.city_id
```

#### Distance Conversion (meters → km)

Raw distances in the source table are stored in **meters**. The query converts them:

```sql
edmjm.pickup_distance / 1000           AS pickup_distance_km
(edmjm.pickup_distance + edmjm.travel_distance) / 1000  AS dropoff_distance
```

#### ATD Computation (minutes)

```sql
EXTRACT(EPOCH FROM (
    order_final_state_timestamp_local - restaurant_offered_timestamp_utc
)) / 60  AS atd
```

#### Country Filter (Mexico)

Only trips where `dim_city.country_name = '{{ country_name }}'` are included (e.g., `'Mexico'`). This makes the pipeline multi-country extensible — the country name is injected at DAG runtime via Airflow Variables.

#### Weekly Dynamic Logic Using `{{ ds }}`

Airflow's `{{ ds }}` macro resolves to the DAG execution date (the Monday the DAG runs). The query filters the previous 7 days:

```sql
WHERE edmjm.date_str::date
    BETWEEN '{{ ds }}'::DATE - INTERVAL '7 days'
    AND     '{{ ds }}'::DATE
```

This guarantees that each weekly run captures exactly the prior week's data regardless of when the DAG is manually triggered or backfilled.

#### Output Table

Results are written to `{schema}.{country}_delivery_trips_info` (e.g., `uber_prod.mx_delivery_trips_info`) and contain:

`territory`, `country_name`, `workflow_uuid`, `driver_uuid`, `delivery_trip_uuid`, `courier_flow`, `restaurant_offered_timestamp_utc`, `order_final_state_timestamp_local`, `eater_request_timestamp_local`, `geo_archetype`, `merchant_surface`, `pickup_distance_km`, `dropoff_distance`, `atd`

---

## 4. ETL Workflow Design

### DAG: `weekly_delivery_etl`

**Schedule:** Every Monday at 00:00 UTC (`0 0 * * 1`)

**Retry Policy:** 2 retries, 5-minute delay between attempts

#### Task Sequence

```
create_schema
     │
     ▼
{country}_weekly_delivery_data
     │
     ▼
{country}_historic_delivery_data
```

**Task 1 — `create_schema`**
Creates the `uber_prod` schema if it does not already exist. This is a safe, idempotent operation.

**Task 2 — `{country}_weekly_delivery_data`**
Executed via `PostgresOperator`. Drops and recreates `uber_prod.{country}_delivery_trips_info` with the output of `weekly_delivery_query.sql` rendered for the current execution date and country.

**Task 3 — `{country}_historic_delivery_data`**
Executed via `PostgresOperator`. Runs `historic_delivery_query.sql`, which:
- Creates `uber_prod.{country}_delivery_trips_historic_info` if it does not exist (including a unique constraint on `(delivery_trip_uuid, batch_date)`)
- Inserts new rows from the weekly snapshot
- On conflict, updates all data columns and sets `updated_at` to the current timestamp

This makes every run fully idempotent: re-triggering the same `{{ ds }}` will update existing rows, not create duplicates.

#### Scalability and Automation

- **Multi-country:** The DAG reads a `countries` Airflow Variable (JSON dict mapping country codes to country names). Adding a new country requires only updating that variable — no code changes.
- **Configurable schedule:** The cron expression is read from an Airflow Variable (`cron`), not hardcoded.
- **Configurable schema:** The target schema is read from an Airflow Variable (`schema`).
- **Connection management:** Database credentials are managed through Airflow Connections (not environment variables), keeping secrets out of code.

---

## 5. Data Cleaning & Preprocessing

Cleaning is performed in `atd_model_training.py` (`_load_and_clean_data` task), which reads from the historical table and applies the following steps in order:

### Handling Missing Values

| Column | Strategy |
|---|---|
| `pickup_distance`, `dropoff_distance` | Create `missing_distance` binary flag first, then fill with **territory-level median** |
| `courier_flow` | Replace `"\N"` sentinel values with `"Unspecified"` |
| `restaurant_offered_timestamp_local` | Rows with a missing value are **dropped** (this timestamp is required for feature derivation) |

Territory-level median imputation is preferred over global median because distance profiles vary significantly across territories. Imputing with the local median preserves these correlations.

### Removing Exact Duplicate Rows

```python
df = df.drop_duplicates()
```

Exact row duplicates can appear when the same trip is captured in multiple weekly batches (e.g., a late-arriving record). Removing them before modeling prevents the model from learning from repeated observations and avoids inflating evaluation metrics.

### Outlier Filtering (Heavy-Tailed ATD)

ATD distributions in delivery data are right-skewed with a heavy tail: most trips complete in 20–40 minutes, but a small fraction take several hours due to edge cases (restaurant closures, courier cancellations, manual overrides). These extreme values distort regression training.

Steps applied:
1. **Negative ATD removed:** Rows where `atd <= 0` are dropped. Negative values indicate a data recording error (final state timestamp precedes the offer timestamp).
2. **Temporal inconsistency removed:** Rows where `restaurant_offered_timestamp_local` is after `order_final_state_timestamp_local` are dropped — this is a data quality violation.

### Timestamp Validation Logic

1. Parse `restaurant_offered_timestamp_utc` and convert to local time by subtracting the country's UTC offset (Mexico: UTC-6, i.e., subtract 6 hours).
2. Parse `eater_request_timestamp_local` and `order_final_state_timestamp_local` as datetime objects.
3. Drop rows where the parsed local offer timestamp is `NaT`.
4. Drop rows where the offer timestamp is chronologically after the final state timestamp.

---

## 6. Feature Engineering

Two feature sets are built to support two modeling scenarios:

| Variant | Use Case |
|---|---|
| **Pre-assignment** | Features available before a courier is dispatched; suitable for early ETA estimates |
| **Post-assignment** | Adds features that become available after a courier accepts the job; more accurate |

### Distance Features

| Feature | Description |
|---|---|
| `pickup_distance` | Distance (km) from courier to restaurant at dispatch |
| `dropoff_distance` | Total distance (km) from courier to restaurant to customer |
| `total_distance` | Sum of pickup and dropoff distances |

### Time Features

| Feature | Description |
|---|---|
| `hour` | Hour of day (0–23) extracted from `restaurant_offered_timestamp_local` |
| `day_of_week` | Day of week (0=Monday … 6=Sunday) |
| `is_weekend` | Binary flag: 1 if Saturday or Sunday |
| `hour_sin` | `sin(2π × hour / 24)` — cyclical encoding |
| `hour_cos` | `cos(2π × hour / 24)` — cyclical encoding |

Cyclical encoding (sin/cos) is used for `hour` so that 23:00 and 00:00 are treated as adjacent by the model rather than maximally distant.

### Dispatch Delay (Post-Assignment Only — Critical Feature)

```python
dispatch_delay = (restaurant_offered_timestamp_local - eater_request_timestamp_local).dt.total_seconds() / 60
```

This is the number of minutes between when the customer placed the order and when the restaurant received it. High dispatch delay is a strong predictor of longer ATD because it indicates operational friction upstream of delivery. It is the single most informative post-assignment feature.

A companion binary flag `offered_equals_request` is set to 1 when both timestamps are identical (which can indicate a data recording artifact rather than a true 0-minute delay).

### Categorical Encoding (One-Hot Encoding)

| Feature | Description |
|---|---|
| `territory` | Geographic operating territory |
| `courier_flow` | Courier assignment method (e.g., pre-positioned, on-demand) |
| `geo_archetype` | Urban density classification of the delivery area |
| `merchant_surface` | Order placement surface (app, web, kiosk, etc.) |

All categorical features are encoded with `OneHotEncoder(handle_unknown="ignore")`. Unknown categories seen at inference time are silently treated as all-zero vectors.

### Preprocessing Pipeline

```
Numeric features (pickup_distance, dropoff_distance, total_distance, hour, day_of_week, dispatch_delay)
    → log1p transform      (reduces right skew)
    → StandardScaler       (zero mean, unit variance)

Categorical features (territory, courier_flow, geo_archetype, merchant_surface)
    → OneHotEncoder

Cyclical features (hour_sin, hour_cos)
    → Passthrough          (already in [-1, 1] range)
```

The full pipeline is serialized as `preprocessing_pipeline_{variant}.pkl` for use at inference time.

### Train/Test Split Strategy

- **80/20 split** with `random_state=42`
- **Stratified** on combinations of `territory × courier_flow × hour_bin` to ensure both sets are representative across all operational subgroups
- Groups with fewer than 10 samples are excluded from stratification to avoid empty strata
- The test set is fixed and never used during training — it is reserved solely for the performance metrics displayed in the dashboard

---

## 7. Model Development

### Target Variable

`ATD` — Actual Time to Delivery in **minutes**, measured from `restaurant_offered_timestamp_local` to `order_final_state_timestamp_local`.

### Models Evaluated

Four regression models are trained and compared for each feature variant:

| Model | Key Hyperparameters | Role |
|---|---|---|
| **Linear Regression** | Default (no regularization) | Interpretable baseline |
| **Random Forest** | `n_estimators=100`, `max_depth=10` | Ensemble baseline, robust to outliers |
| **Gradient Boosting** | `n_estimators=100`, `learning_rate=0.1`, `max_depth=3` | Sequential boosting, handles interactions |
| **XGBoost** | `n_estimators=200`, `learning_rate=0.1`, `max_depth=6`, `subsample=0.8`, `colsample_bytree=0.8` | Regularized boosting, typically best performer |

### Why Tree-Based Models

ATD is determined by a combination of non-linear factors: distance interacts with time-of-day, territory-specific courier availability interacts with order volume, and dispatch delay has a threshold effect. Linear Regression cannot capture these interactions without explicit polynomial feature creation.

Tree-based models (Random Forest, Gradient Boosting, XGBoost) learn these interactions automatically and are robust to the moderate outliers remaining after cleaning. Gradient Boosting and XGBoost also benefit from regularization parameters (`subsample`, `colsample_bytree`) that reduce variance on noisy delivery data.

### Train/Test Strategy (No Leakage)

- The preprocessing pipeline is **fit only on the training set** and then applied to both train and test sets.
- Stratified splitting ensures test-set coverage of all territory/flow/hour combinations.
- The test set is held out from all model selection decisions — the best model is selected by RMSE on the test set only once, after all models are trained.
- `dispatch_delay` (post-assignment only) is computed from raw timestamps before the split; it contains no future information because both timestamps precede the ATD measurement window.

### Model Selection

The best model per variant is selected by **lowest RMSE on the test set** and serialized as `best_model_{variant}.pkl`.

---

## 8. Evaluation Metrics

Three metrics are computed for each model on the held-out test set:

| Metric | Formula | What It Measures |
|---|---|---|
| **MAE** (Mean Absolute Error) | `mean(|y - ŷ|)` | Average prediction error in minutes; directly interpretable by operations teams |
| **RMSE** (Root Mean Squared Error) | `sqrt(mean((y - ŷ)²))` | Penalizes large errors more heavily; critical because very late deliveries are disproportionately damaging to customer experience |
| **R²** (Coefficient of Determination) | `1 - SS_res / SS_tot` | Proportion of ATD variance explained by the model; useful for comparing models on the same dataset |

**Why MAE?** Operations teams think in minutes. A MAE of 4 minutes is immediately interpretable: "our model is off by 4 minutes on average."

**Why RMSE?** ATD has business asymmetry — being 20 minutes late is far worse than being 5 minutes early. RMSE's quadratic penalty aligns with this: it gives more weight to the large errors that cause customer dissatisfaction and complaint escalations.

**Why R²?** Provides a normalized baseline for model quality. An R² of 0.75 means the model explains 75% of the variance in delivery times, regardless of the absolute scale of ATD.

The dashboard also reports **P50 and P90 absolute errors** for a more complete picture of the error distribution, since mean-based metrics can be misleading when errors are skewed.

---

## 9. Dashboard

The Streamlit dashboard (`dashboard/app.py`) provides an interactive interface for operations and product stakeholders to explore delivery performance and model output.

### Data Source

The dashboard loads `notebooks/df_clean.csv` — the cleaned, feature-enriched dataset produced by the ETL and cleaning pipeline.

### Sidebar Filters

All sections (except the model performance metrics) respond to the following filters:

- **Territory** — multiselect
- **Courier Flow** — multiselect
- **Geo Archetype** — multiselect
- **Merchant Surface** — multiselect
- **Hour of Day** — range slider (0–23)

### Sections

#### 1. Overview

Displays aggregate KPIs for the filtered dataset:

- Average ATD (minutes)
- Median ATD (minutes)
- P90 ATD (minutes) — the 90th percentile; a common SLA benchmark
- Total trip count

Also shows an ATD distribution histogram and a per-territory trip count table.

#### 2. Exploratory Analysis (EDA)

| Chart | Purpose |
|---|---|
| ATD Histogram with P50/P90 lines | Understand the shape of the distribution |
| ATD Boxplot by Territory | Compare median and spread across geographies |
| ATD vs Dropoff Distance (scatter) | Validate the distance–time relationship |
| ATD vs Pickup Distance (scatter) | Identify courier positioning effects |
| Average ATD by Hour of Day | Detect peak-hour congestion patterns |
| Average ATD by Day of Week | Identify day-of-week demand cycles |

Scatter charts sample up to 5,000 points for rendering performance.

#### 3. Performance Analysis

A dropdown selects between the **pre-assignment** and **post-assignment** model variants.

Metrics displayed (on the fixed, unfiltered test set):

- MAE, RMSE, P50 absolute error, P90 absolute error

Visualizations:

| Chart | Purpose |
|---|---|
| Actual vs Predicted scatter | Assess overall fit; perfect predictions lie on the diagonal |
| Residual plot (predicted vs signed error) | Detect systematic bias or heteroscedasticity |
| Signed error distribution | Understand whether the model tends to over- or under-predict |
| Absolute error distribution with P50/P90 | Communicate prediction reliability to stakeholders |

#### 4. Model Predictions

An interactive form accepts a trip's features and returns a single ATD forecast:

- Pickup distance, dropoff distance, hour of day
- Territory, courier flow, geo archetype, merchant surface
- (Post-assignment) Dispatch delay in minutes, offered=request flag

### How Stakeholders Use It

- **Operations managers** use the Overview and EDA sections to monitor delivery health, spot territory regressions, and detect anomalous hours.
- **Data scientists** use the Performance section to validate model quality before deploying updates.
- **Product managers** use the Predictions tab to explore "what-if" scenarios (e.g., how much does ATD change if pickup distance increases by 2 km?).

---

## 10. How to Run the Project

### Prerequisites

- Docker and Docker Compose installed
- Python 3.10+
- Git

### Step 1 — Clone the Repository

```bash
git clone https://github.com/FernandoCapporal/delivery-time-analytics.git
cd delivery-time-analytics
```

### Step 2 — Create the Environment File

```bash
cp .env.example .env
```

Open `.env` and fill in the required values:

```
AIRFLOW_UID=<output of: id -u>
AIRFLOW_GID=<output of: id -g>
POSTGRES_USER=postgres
POSTGRES_PASSWORD=<your password>
POSTGRES_DB=delivery_db
PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD=admin
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
DB_CONN_STRING=postgresql://postgres:<your password>@localhost:5432/delivery_db
```

### Step 3 — Start the Infrastructure

```bash
mkdir -p dags logs plugins
echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose up -d
```

Wait for all services to report healthy (approximately 60–90 seconds):

```bash
docker compose ps
```

| Service | URL | Default Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| pgAdmin | http://localhost:5050 | admin@admin.com / admin |
| PostgreSQL | localhost:5432 | postgres / \<your password\> |

### Step 4 — Seed the Database (Mock Data)

Open and run `db/mock_init.ipynb` to populate the source tables with mock data, or connect to your production source tables by updating the Airflow PostgreSQL connection in the Airflow UI under **Admin → Connections**.

### Step 5 — Trigger the ETL Pipeline

In the Airflow UI:
1. Enable the `weekly_delivery_etl` DAG
2. Trigger it manually for a backfill date, or wait for the Monday 00:00 UTC schedule
3. Once complete, enable the `atd_model_training` DAG

### Step 6 — Install Python Dependencies

```bash
pip install -r requirements.txt
```

For an isolated environment (recommended):

```bash
# Using conda
conda create -n delivery-analytics python=3.10
conda activate delivery-analytics
pip install -r requirements.txt

# Using venv
python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Step 7 — Run Notebooks (Optional)

```bash
jupyter notebook notebooks/
```

Notebooks should be run in order:
1. `01_eda.ipynb` — explore raw data
2. `02_feature_engineering.ipynb` — prototype feature logic
3. `03_atd_model.ipynb` — train and evaluate models

The model training notebook will produce `df_clean.csv` and `.pkl` artifacts consumed by the dashboard.

### Step 8 — Launch the Dashboard

```bash
streamlit run dashboard/app.py
```

The dashboard opens at http://localhost:8501.

> **Note:** The dashboard requires `notebooks/df_clean.csv` and the model `.pkl` files to be present. These are generated by either the Airflow `atd_model_training` DAG or by running `03_atd_model.ipynb` manually.

---

## 11. Reproducibility

To reproduce results end-to-end:

1. **Same data:** Use the same `BC_A&A_with_ATD.csv` source file or re-extract from the source tables using the same `{{ ds }}` date range.

2. **Same random state:** All stochastic operations use `random_state=42`:
   - Train/test split (`train_test_split(..., random_state=42)`)
   - Random Forest (`RandomForestRegressor(..., random_state=42)`)
   - XGBoost (`XGBRegressor(..., random_state=42)`)

3. **Same cleaning order:** The cleaning steps in `atd_model_training.py` must be applied in the documented sequence. Deduplication before imputation, timestamp validation before ATD filtering.

4. **Same preprocessing pipeline:** The `preprocessing_pipeline_{variant}.pkl` file must be used for inference — fitting a new pipeline on new data will produce different scaling parameters and different one-hot column ordering.

5. **Pinned dependencies:** `requirements.txt` pins all package versions. Use `pip install -r requirements.txt` exactly as written to avoid version drift.

6. **Environment variables:** Ensure `DB_CONN_STRING` points to the correct database and that the `uber_prod` schema is populated before running the dashboard or model training DAG.

---

## 12. Notes & Assumptions

### Modeling Assumptions

- **ATD is measured from offer to final state.** The target variable does not include time spent in the restaurant kitchen. It reflects end-to-end courier time only.
- **Negative and zero ATD values are recording errors.** No business scenario produces a zero or negative delivery time; these rows are dropped rather than investigated further.
- **Dispatch delay can be negative** when timestamps are recorded out of order due to system latency. Negative dispatch delay values produce `NaN` after `log1p` transformation and are filtered from the training set.
- **Territory-level median imputation is sufficient** for missing distances. A more sophisticated imputation (e.g., based on pickup coordinates) was not implemented because the missingness rate is low and territory captures most of the geographic variance.

### Data Assumptions

- The source tables (`lea_trips_scope_atd_consolidation_v2`, `eats_dispatch_metrics_job_message`, etc.) are assumed to be available in the connected PostgreSQL instance. In the local Docker environment, these are populated with mock data via `db/mock_init.ipynb`.
- Distance columns in the source table are in **meters**. Division by 1000 is applied in SQL.
- Mexico's UTC offset is hardcoded as **UTC-6** for timestamp localization. This does not account for Daylight Saving Time transitions. For production use, a timezone-aware conversion (e.g., `pytz` with the `America/Mexico_City` zone) is recommended.
- `courier_flow` values of `"\N"` are PostgreSQL's NULL representation in CSV export mode. They are normalized to `"Unspecified"` to avoid them being treated as a meaningful category.

### Dashboard Assumptions

- The dashboard reads from a static CSV (`df_clean.csv`) rather than querying the database live. This means it reflects a point-in-time snapshot of the data. For a production deployment, the data loading layer in `dashboard/data.py` would be replaced with a live database query.
- Model performance metrics displayed in the Performance section are computed on the fixed test set generated during training. They are not re-computed on the filtered dataset — this is intentional to prevent metric inflation from filtering out hard-to-predict trips.

### Pipeline Assumptions

- The `atd_model_training` DAG uses an `ExternalTaskSensor` with a 2-hour execution delta to wait for `weekly_delivery_etl` to complete before reading from the historical table. If the ETL takes longer than 2 hours, the sensor will time out and the training run will fail.
- S3 upload in `upload_artifacts_to_s3` requires a valid AWS connection (`aws_conn_id`) configured in Airflow. In local development this task can be disabled or replaced with local file storage.

---

## Environment Variables

| Variable | Description |
|---|---|
| `AIRFLOW_UID` | Host user UID for Airflow Docker volume permissions |
| `AIRFLOW_GID` | Host group GID |
| `POSTGRES_USER` | PostgreSQL username |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `POSTGRES_DB` | Target database name |
| `PGADMIN_DEFAULT_EMAIL` | pgAdmin login email |
| `PGADMIN_DEFAULT_PASSWORD` | pgAdmin login password |
| `_AIRFLOW_WWW_USER_USERNAME` | Airflow web UI username |
| `_AIRFLOW_WWW_USER_PASSWORD` | Airflow web UI password |
| `DB_CONN_STRING` | SQLAlchemy connection string for Streamlit (e.g., `postgresql://user:pass@localhost:5432/db`) |

---

## License

MIT
