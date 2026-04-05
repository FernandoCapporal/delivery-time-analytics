"""
DAG: atd_model_training
Description: Weekly ML training pipeline that reads the historical delivery trips
             table produced by weekly_delivery_etl, applies EDA cleaning,
             feature engineering, trains pre-assignment and post-assignment ATD
             models, and uploads model + preprocessing pipeline artifacts to S3.
Schedule: Every Monday at 02:00 UTC (runs after weekly_delivery_etl completes)
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


process_name = "atd_model_training"

# ─────────────────────────────────────────────────────────────
# Airflow Variables
# ─────────────────────────────────────────────────────────────
dag_variable     = Variable.get(process_name, default_var={}, deserialize_json=True)
countries        = dag_variable.get("countries", {"mx": "Mexico"})
schema           = dag_variable.get("schema", "uber_prod")
cron             = dag_variable.get("cron", "0 2 * * 1")
s3_bucket        = dag_variable.get("s3_bucket", "uber-delivery-models")
s3_prefix        = dag_variable.get("s3_prefix", "atd_models")
postgres_conn_id = dag_variable.get("postgres_conn_id", "postgres_local")
aws_conn_id      = dag_variable.get("aws_conn_id", "aws_default")

# UTC offset per country for restaurant_offered_timestamp conversion
# (hours to subtract from UTC to obtain local time)
utc_offsets = dag_variable.get("utc_offsets", {"mx": 6})

# ─────────────────────────────────────────────────────────────
# Default args
# ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "luis.caporal",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ─────────────────────────────────────────────────────────────
# Helper — temp directory for a given country / execution date
# ─────────────────────────────────────────────────────────────
def _tmp_dir(country: str, ds: str) -> str:
    return f"/tmp/{process_name}/{country}/{ds}"


# ─────────────────────────────────────────────────────────────
# Task 1: Load data from historic table and apply EDA cleaning
# ─────────────────────────────────────────────────────────────
def _load_and_clean_data(country, schema, postgres_conn_id, utc_offset, ds, **kwargs):
    """
    Pulls all historical rows from {schema}.{country}_delivery_trips_historic_info,
    applies the same cleaning steps developed in 01_eda.ipynb, and persists the
    cleaned DataFrame to a local CSV so downstream tasks can consume it.
    """
    import logging
    from datetime import timedelta as td

    import numpy as np
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # ─── Load ────────────────────────────────────────────────
    hook  = PostgresHook(postgres_conn_id=postgres_conn_id)
    query = f"""
        SELECT *
        FROM {schema}.{country}_delivery_trips_historic_info
        WHERE batch_date <= '{ds}'::DATE
    """
    df = hook.get_pandas_df(query)
    logging.info("Loaded %d rows from historic table", len(df))

    # ─── Rename to notebook convention ───────────────────────
    df = df.rename(columns={
        "pickup_distance_km":  "pickup_distance",
        "dropoff_distance_km": "dropoff_distance",
        "atd":                 "ATD",
    })

    # ─── Type corrections ────────────────────────────────────
    df["pickup_distance"]  = pd.to_numeric(df["pickup_distance"],  errors="coerce")
    df["dropoff_distance"] = pd.to_numeric(df["dropoff_distance"], errors="coerce")

    for ts_col in [
        "restaurant_offered_timestamp_utc",
        "order_final_state_timestamp_local",
        "eater_request_timestamp_local",
    ]:
        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")

    # Convert offered timestamp from UTC to local
    df["restaurant_offered_timestamp_local"] = (
        df["restaurant_offered_timestamp_utc"] - td(hours=utc_offset)
    )
    df.drop(columns=["restaurant_offered_timestamp_utc"], inplace=True)

    # ─── Remove exact duplicates ─────────────────────────────
    before = len(df)
    df = df.drop_duplicates()
    logging.info("Dropped %d exact duplicate rows", before - len(df))

    # ─── Drop identifier / batch metadata columns ─────────────
    drop_cols = [
        c for c in [
            "region", "country_name", "workflow_uuid", "driver_uuid",
            "delivery_trip_uuid", "batch_date", "week_start", "week_end",
            "inserted_at", "updated_at",
        ]
        if c in df.columns
    ]
    df.drop(columns=drop_cols, inplace=True)

    # ─── Missing value flags ──────────────────────────────────
    df["missing_distance"] = df["pickup_distance"].isna().astype(int)

    # ─── Impute distances by territory median ─────────────────
    df["pickup_distance"] = df.groupby("territory")["pickup_distance"].transform(
        lambda x: x.fillna(x.median())
    )
    df["dropoff_distance"] = df.groupby("territory")["dropoff_distance"].transform(
        lambda x: x.fillna(x.median())
    )

    # ─── Normalise courier_flow sentinel value ────────────────
    df["courier_flow"] = df["courier_flow"].replace("\\N", "Unespecified")

    # ─── Drop rows where offered timestamp is missing ─────────
    before = len(df)
    df = df.dropna(subset=["restaurant_offered_timestamp_local"])
    logging.info("Dropped %d rows with null restaurant_offered_timestamp_local", before - len(df))

    # ─── Timestamp consistency flags ─────────────────────────
    df["offered_equals_request"] = (
        df["restaurant_offered_timestamp_local"] == df["eater_request_timestamp_local"]
    ).astype(int)

    df["offered_after_final"] = (
        df["restaurant_offered_timestamp_local"] > df["order_final_state_timestamp_local"]
    ).astype(int)

    # ─── Filter out temporal inconsistencies & invalid ATD ───
    df = df[df["offered_after_final"] == 0]
    df = df[df["ATD"] > 0]

    df.drop(columns=["offered_after_final"], inplace=True)

    logging.info("Clean dataset shape: %s", df.shape)

    # ─── Persist ─────────────────────────────────────────────
    tmp = _tmp_dir(country, ds)
    os.makedirs(tmp, exist_ok=True)

    out_path = os.path.join(tmp, "df_clean.csv")
    df.to_csv(out_path, index=False)

    return out_path


# ─────────────────────────────────────────────────────────────
# Task 2a: Feature engineering — pre-assignment model
# ─────────────────────────────────────────────────────────────
def _build_pre_assign_features(country, ds, **kwargs):
    """
    Builds the feature set for the pre-assignment model (no dispatch delay).
    Applies stratified train/test split, fits the preprocessing pipeline, and
    saves transformed arrays + the pipeline to disk.
    """
    import logging

    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.compose import ColumnTransformer
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, StandardScaler

    ti       = kwargs["ti"]
    df_path  = ti.xcom_pull(task_ids=f"load_and_clean_data_{country}")
    tmp      = _tmp_dir(country, ds)

    df = pd.read_csv(df_path)
    df["eater_request_timestamp_local"] = pd.to_datetime(
        df["eater_request_timestamp_local"]
    )

    # ─── Temporal features ───────────────────────────────────
    df["hour"]        = df["eater_request_timestamp_local"].dt.hour
    df["day_of_week"] = df["eater_request_timestamp_local"].dt.dayofweek
    df["is_weekend"]  = df["day_of_week"].isin([5, 6]).astype(int)
    df["hour_sin"]    = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"]    = np.cos(2 * np.pi * df["hour"] / 24)

    # ─── Distance feature ────────────────────────────────────
    df["total_distance"] = df["pickup_distance"] + df["dropoff_distance"]

    # ─── Feature groups ──────────────────────────────────────
    num_cols      = ["pickup_distance", "dropoff_distance", "total_distance"]
    cat_cols      = ["territory", "courier_flow", "geo_archetype", "merchant_surface"]
    cyclical_cols = ["hour_sin", "hour_cos"]

    # ─── Stratification ──────────────────────────────────────
    df["hour_bin"] = df["hour"] // 3
    df["strata"]   = (
        df["territory"].astype(str) + "_"
        + df["courier_flow"].astype(str) + "_"
        + df["hour_bin"].astype(str)
    )
    strata_counts = df["strata"].value_counts()
    valid_strata  = strata_counts[strata_counts > 10].index
    df            = df[df["strata"].isin(valid_strata)]

    # ─── Train / test split ──────────────────────────────────
    X = df[num_cols + cat_cols + cyclical_cols]
    y = df["ATD"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=df["strata"]
    )

    # ─── Preprocessing pipeline ──────────────────────────────
    numeric_pipeline = Pipeline([
        ("log",    FunctionTransformer(np.log1p)),
        ("scaler", StandardScaler()),
    ])

    preprocessor = ColumnTransformer([
        ("num", numeric_pipeline,             num_cols),
        ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
        ("cyc", "passthrough",                cyclical_cols),
    ])

    pipeline = Pipeline([("preprocessing", preprocessor)])

    X_train_t = pipeline.fit_transform(X_train)
    X_test_t  = pipeline.transform(X_test)

    logging.info("[pre_assign] Train shape: %s | Test shape: %s", X_train_t.shape, X_test_t.shape)

    # ─── Persist ─────────────────────────────────────────────
    joblib.dump(X_train_t, os.path.join(tmp, "X_train_pre_assign.pkl"))
    joblib.dump(X_test_t,  os.path.join(tmp, "X_test_pre_assign.pkl"))
    joblib.dump(y_train,   os.path.join(tmp, "y_train_pre_assign.pkl"))
    joblib.dump(y_test,    os.path.join(tmp, "y_test_pre_assign.pkl"))
    joblib.dump(pipeline,  os.path.join(tmp, "preprocessing_pipeline_pre_assign.pkl"))

    return tmp


# ─────────────────────────────────────────────────────────────
# Task 2b: Feature engineering — post-assignment model
# ─────────────────────────────────────────────────────────────
def _build_post_assign_features(country, ds, **kwargs):
    """
    Builds the feature set for the post-assignment model.
    Adds dispatch_delay (minutes from eater request to driver assignment) and the
    offered_equals_request binary flag on top of the pre-assignment features.
    """
    import logging

    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.compose import ColumnTransformer
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, StandardScaler

    ti      = kwargs["ti"]
    df_path = ti.xcom_pull(task_ids=f"load_and_clean_data_{country}")
    tmp     = _tmp_dir(country, ds)

    df = pd.read_csv(df_path)
    df["eater_request_timestamp_local"]      = pd.to_datetime(df["eater_request_timestamp_local"])
    df["restaurant_offered_timestamp_local"] = pd.to_datetime(df["restaurant_offered_timestamp_local"])

    # ─── Temporal features ───────────────────────────────────
    df["hour"]        = df["eater_request_timestamp_local"].dt.hour
    df["day_of_week"] = df["eater_request_timestamp_local"].dt.dayofweek
    df["is_weekend"]  = df["day_of_week"].isin([5, 6]).astype(int)
    df["hour_sin"]    = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"]    = np.cos(2 * np.pi * df["hour"] / 24)

    # ─── Distance feature ────────────────────────────────────
    df["total_distance"] = df["pickup_distance"] + df["dropoff_distance"]

    # ─── Dispatch delay (minutes from request to driver assignment) ──
    df["dispatch_delay"] = (
        df["restaurant_offered_timestamp_local"] - df["eater_request_timestamp_local"]
    ).dt.total_seconds() / 60

    # ─── Binary flag ─────────────────────────────────────────
    df["offered_equals_request"] = df["offered_equals_request"].astype(int)

    # ─── Feature groups ──────────────────────────────────────
    num_cols      = ["pickup_distance", "dropoff_distance", "total_distance", "dispatch_delay"]
    cat_cols      = ["territory", "courier_flow", "geo_archetype", "merchant_surface"]
    binary_cols   = ["offered_equals_request"]
    cyclical_cols = ["hour_sin", "hour_cos"]

    # ─── Stratification ──────────────────────────────────────
    df["hour_bin"] = df["hour"] // 3
    df["strata"]   = (
        df["territory"].astype(str) + "_"
        + df["courier_flow"].astype(str) + "_"
        + df["hour_bin"].astype(str)
    )
    strata_counts = df["strata"].value_counts()
    valid_strata  = strata_counts[strata_counts > 10].index
    df            = df[df["strata"].isin(valid_strata)]

    # ─── Train / test split ──────────────────────────────────
    X = df[num_cols + cat_cols + binary_cols + cyclical_cols]
    y = df["ATD"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=df["strata"]
    )

    # ─── Preprocessing pipeline ──────────────────────────────
    numeric_pipeline = Pipeline([
        ("log",    FunctionTransformer(np.log1p)),
        ("scaler", StandardScaler()),
    ])

    preprocessor = ColumnTransformer([
        ("num", numeric_pipeline,                        num_cols),
        ("cat", OneHotEncoder(handle_unknown="ignore"),  cat_cols),
        ("bin", "passthrough",                           binary_cols),
        ("cyc", "passthrough",                           cyclical_cols),
    ])

    pipeline = Pipeline([("preprocessing", preprocessor)])

    X_train_t = pipeline.fit_transform(X_train)
    X_test_t  = pipeline.transform(X_test)

    logging.info("[post_assign] Train shape: %s | Test shape: %s", X_train_t.shape, X_test_t.shape)

    # ─── Persist ─────────────────────────────────────────────
    joblib.dump(X_train_t, os.path.join(tmp, "X_train_post_assign.pkl"))
    joblib.dump(X_test_t,  os.path.join(tmp, "X_test_post_assign.pkl"))
    joblib.dump(y_train,   os.path.join(tmp, "y_train_post_assign.pkl"))
    joblib.dump(y_test,    os.path.join(tmp, "y_test_post_assign.pkl"))
    joblib.dump(pipeline,  os.path.join(tmp, "preprocessing_pipeline_post_assign.pkl"))

    return tmp


# ─────────────────────────────────────────────────────────────
# Task 3: Train and evaluate models, persist the best one
# ─────────────────────────────────────────────────────────────
def _train_model(variant, country, ds, **kwargs):
    """
    Trains LinearRegression, RandomForest, GradientBoosting, and XGBoost against
    the pre-transformed arrays for `variant` (pre_assign | post_assign).
    Selects the model with the lowest RMSE and saves it to disk.
    """
    import logging

    import joblib
    import numpy as np
    from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from xgboost import XGBRegressor

    tmp = _tmp_dir(country, ds)

    # ─── Load pre-transformed data ────────────────────────────
    X_train = joblib.load(os.path.join(tmp, f"X_train_{variant}.pkl"))
    X_test  = joblib.load(os.path.join(tmp, f"X_test_{variant}.pkl"))
    y_train = joblib.load(os.path.join(tmp, f"y_train_{variant}.pkl"))
    y_test  = joblib.load(os.path.join(tmp, f"y_test_{variant}.pkl"))

    # ─── Filter NaN rows (log1p of negative dispatch_delay) ──
    train_mask = ~np.isnan(X_train).any(axis=1)
    test_mask  = ~np.isnan(X_test).any(axis=1)
    X_train, y_train = X_train[train_mask], y_train[train_mask]
    X_test,  y_test  = X_test[test_mask],   y_test[test_mask]

    logging.info("[%s] After NaN filter — Train: %s | Test: %s", variant, X_train.shape, X_test.shape)

    # ─── Model catalogue ─────────────────────────────────────
    models = {
        "LinearRegression": LinearRegression(),
        "RandomForest": RandomForestRegressor(
            n_estimators=100, max_depth=10, random_state=42, n_jobs=-1
        ),
        "GradientBoosting": GradientBoostingRegressor(
            n_estimators=100, learning_rate=0.1, max_depth=3
        ),
        "XGBoost": XGBRegressor(
            n_estimators=200, learning_rate=0.1, max_depth=6,
            subsample=0.8, colsample_bytree=0.8, random_state=42, n_jobs=-1
        ),
    }

    # ─── Train, evaluate, collect results ────────────────────
    results = {}

    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        mae  = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2   = r2_score(y_test, y_pred)

        results[name] = {"model": model, "MAE": mae, "RMSE": rmse, "R2": r2}
        logging.info("[%s] %s — MAE: %.2f | RMSE: %.2f | R2: %.4f", variant, name, mae, rmse, r2)

    # ─── Select best by RMSE ─────────────────────────────────
    best_name  = min(results, key=lambda n: results[n]["RMSE"])
    best_model = results[best_name]["model"]

    logging.info(
        "[%s] Best model: %s — RMSE: %.2f", variant, best_name, results[best_name]["RMSE"]
    )

    # ─── Persist ─────────────────────────────────────────────
    joblib.dump(best_model, os.path.join(tmp, f"best_model_{variant}.pkl"))


# ─────────────────────────────────────────────────────────────
# Task 4: Upload model and pipeline artifacts to S3
# ─────────────────────────────────────────────────────────────
def _upload_artifacts_to_s3(country, ds, s3_bucket, s3_prefix, aws_conn_id, **kwargs):
    """
    Uploads the four serialised artifacts to S3:
      - best_model_pre_assign.pkl
      - best_model_post_assign.pkl
      - preprocessing_pipeline_pre_assign.pkl
      - preprocessing_pipeline_post_assign.pkl

    S3 key pattern: {s3_prefix}/{country}/{ds}/{filename}
    """
    import logging

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    tmp = _tmp_dir(country, ds)

    artifacts = [
        "best_model_pre_assign.pkl",
        "best_model_post_assign.pkl",
        "preprocessing_pipeline_pre_assign.pkl",
        "preprocessing_pipeline_post_assign.pkl",
    ]

    hook = S3Hook(aws_conn_id=aws_conn_id)

    for filename in artifacts:
        local_path = os.path.join(tmp, filename)
        s3_key     = f"{s3_prefix}/{country}/{ds}/{filename}"

        hook.load_file(
            filename=local_path,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True,
        )
        logging.info("Uploaded %s → s3://%s/%s", filename, s3_bucket, s3_key)


# ─────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────
with DAG(
    dag_id=process_name,
    description="Weekly ML pipeline: feature engineering + ATD model training + S3 upload",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=cron,  # Every Monday at 02:00 UTC
    catchup=False,
    tags=["delivery", "ml", "training", "weekly"],
) as dag:

    # ─────────────────────────────────────────────────────────
    # Task 0: Wait for the ETL DAG to finish on the same date
    # ─────────────────────────────────────────────────────────
    wait_for_etl = ExternalTaskSensor(
        task_id="wait_for_weekly_delivery_etl",
        external_dag_id="weekly_delivery_etl",
        external_task_id=None,          # Wait for the full DAG to succeed
        execution_delta=timedelta(hours=2),  # ETL runs at 00:00, this DAG at 02:00
        timeout=3600,                   # 1-hour timeout
        poke_interval=60,
        mode="reschedule",
    )

    for country, country_name in countries.items():

        utc_offset = utc_offsets.get(country, 0)

        # ─────────────────────────────────────────────────────
        # Task 1: Load historic data and apply EDA cleaning
        # ─────────────────────────────────────────────────────
        load_and_clean = PythonOperator(
            task_id=f"load_and_clean_data_{country}",
            python_callable=_load_and_clean_data,
            op_kwargs={
                "country":          country,
                "schema":           schema,
                "postgres_conn_id": postgres_conn_id,
                "utc_offset":       utc_offset,
                "ds":               "{{ ds }}",
            },
        )

        # ─────────────────────────────────────────────────────
        # Task 2a: Feature engineering — pre-assignment
        # ─────────────────────────────────────────────────────
        pre_assign_features = PythonOperator(
            task_id=f"build_pre_assign_features_{country}",
            python_callable=_build_pre_assign_features,
            op_kwargs={
                "country": country,
                "ds":      "{{ ds }}",
            },
        )

        # ─────────────────────────────────────────────────────
        # Task 2b: Feature engineering — post-assignment
        # ─────────────────────────────────────────────────────
        post_assign_features = PythonOperator(
            task_id=f"build_post_assign_features_{country}",
            python_callable=_build_post_assign_features,
            op_kwargs={
                "country": country,
                "ds":      "{{ ds }}",
            },
        )

        # ─────────────────────────────────────────────────────
        # Task 3a: Train ATD model — pre-assignment
        # ─────────────────────────────────────────────────────
        train_pre = PythonOperator(
            task_id=f"train_pre_assign_model_{country}",
            python_callable=_train_model,
            op_kwargs={
                "variant": "pre_assign",
                "country": country,
                "ds":      "{{ ds }}",
            },
        )

        # ─────────────────────────────────────────────────────
        # Task 3b: Train ATD model — post-assignment
        # ─────────────────────────────────────────────────────
        train_post = PythonOperator(
            task_id=f"train_post_assign_model_{country}",
            python_callable=_train_model,
            op_kwargs={
                "variant": "post_assign",
                "country": country,
                "ds":      "{{ ds }}",
            },
        )

        # ─────────────────────────────────────────────────────
        # Task 4: Upload all artifacts to S3
        # ─────────────────────────────────────────────────────
        upload_to_s3 = PythonOperator(
            task_id=f"upload_artifacts_to_s3_{country}",
            python_callable=_upload_artifacts_to_s3,
            op_kwargs={
                "country":    country,
                "ds":         "{{ ds }}",
                "s3_bucket":  s3_bucket,
                "s3_prefix":  s3_prefix,
                "aws_conn_id": aws_conn_id,
            },
        )

        # ─────────────────────────────────────────────────────
        # Task dependencies
        # ─────────────────────────────────────────────────────
        (
            wait_for_etl
            >> load_and_clean
            >> [pre_assign_features, post_assign_features]
        )

        pre_assign_features  >> train_pre
        post_assign_features >> train_post

        [train_pre, train_post] >> upload_to_s3
