"""Model loading, prediction, and evaluation utilities."""
import os

import numpy as np
import pandas as pd
import streamlit as st

_DIR = os.path.dirname(__file__)
ARTIFACTS_DIR = os.path.join(_DIR, "..", "notebooks")

PRE_ASSIGN_FEATURE_COLS = [
    "pickup_distance", "dropoff_distance", "total_distance",
    "territory", "courier_flow", "geo_archetype", "merchant_surface",
    "hour_sin", "hour_cos",
]

POST_ASSIGN_FEATURE_COLS = [
    "pickup_distance", "dropoff_distance", "total_distance",
    "dispatch_delay",
    "territory", "courier_flow", "geo_archetype", "merchant_surface",
    "offered_equals_request",
    "hour_sin", "hour_cos",
]


@st.cache_resource(show_spinner="Loading model artifacts...")
def load_model_artifacts(variant: str, artifacts_dir: str = ARTIFACTS_DIR):
    """
    Load the trained model, test features, and test labels for `variant`.

    Parameters
    ----------
    variant : str
        One of ``"pre_assign"`` or ``"post_assign"``.
    artifacts_dir : str
        Directory containing the pkl files.

    Returns
    -------
    tuple[model, X_test, y_test] or None if files are missing.
    """
    import joblib

    model_path = os.path.join(artifacts_dir, f"best_model_{variant}.pkl")
    x_path = os.path.join(artifacts_dir, f"X_test_transformed_{variant}.pkl")
    y_path = os.path.join(artifacts_dir, f"y_test_{variant}.pkl")

    for path in (model_path, x_path, y_path):
        if not os.path.exists(path):
            return None

    model = joblib.load(model_path)
    X_test = joblib.load(x_path)
    y_test = np.asarray(joblib.load(y_path))

    return model, X_test, y_test


@st.cache_resource(show_spinner="Loading preprocessing pipeline...")
def load_pipeline(variant: str, artifacts_dir: str = ARTIFACTS_DIR):
    """Load the sklearn preprocessing pipeline for `variant`."""
    import joblib

    path = os.path.join(
        artifacts_dir, f"preprocessing_pipeline_{variant}.pkl"
    )
    if not os.path.exists(path):
        return None
    return joblib.load(path)


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """Return MAE, RMSE, P50 error, and P90 absolute error."""
    errors = np.abs(y_true - y_pred)
    return {
        "MAE": float(np.mean(errors)),
        "RMSE": float(np.sqrt(np.mean((y_true - y_pred) ** 2))),
        "P50 Abs Error": float(np.percentile(errors, 50)),
        "P90 Abs Error": float(np.percentile(errors, 90)),
    }


def build_pre_assign_features(
    pickup_distance: float,
    dropoff_distance: float,
    territory: str,
    courier_flow: str,
    geo_archetype: str,
    merchant_surface: str,
    hour: int,
) -> pd.DataFrame:
    """Construct a single-row feature DataFrame for the pre-assign model."""
    total_distance = pickup_distance + dropoff_distance
    hour_sin = np.sin(2 * np.pi * hour / 24)
    hour_cos = np.cos(2 * np.pi * hour / 24)

    return pd.DataFrame([{
        "pickup_distance": pickup_distance,
        "dropoff_distance": dropoff_distance,
        "total_distance": total_distance,
        "territory": territory,
        "courier_flow": courier_flow,
        "geo_archetype": geo_archetype,
        "merchant_surface": merchant_surface,
        "hour_sin": hour_sin,
        "hour_cos": hour_cos,
    }])


def build_post_assign_features(
    pickup_distance: float,
    dropoff_distance: float,
    territory: str,
    courier_flow: str,
    geo_archetype: str,
    merchant_surface: str,
    hour: int,
    dispatch_delay: float,
    offered_equals_request: int,
) -> pd.DataFrame:
    """Construct a single-row feature DataFrame for the post-assign model."""
    total_distance = pickup_distance + dropoff_distance
    hour_sin = np.sin(2 * np.pi * hour / 24)
    hour_cos = np.cos(2 * np.pi * hour / 24)

    return pd.DataFrame([{
        "pickup_distance": pickup_distance,
        "dropoff_distance": dropoff_distance,
        "total_distance": total_distance,
        "dispatch_delay": dispatch_delay,
        "territory": territory,
        "courier_flow": courier_flow,
        "geo_archetype": geo_archetype,
        "merchant_surface": merchant_surface,
        "offered_equals_request": offered_equals_request,
        "hour_sin": hour_sin,
        "hour_cos": hour_cos,
    }])


def predict(pipeline, model, features: pd.DataFrame) -> float:
    """Apply preprocessing pipeline and return the model's ATD prediction."""
    X_transformed = pipeline.transform(features)
    return float(model.predict(X_transformed)[0])
