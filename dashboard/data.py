"""Data loading, caching, and filtering utilities."""
import os

import numpy as np
import pandas as pd
import streamlit as st

_DIR = os.path.dirname(__file__)
DEFAULT_DATA_PATH = os.path.join(_DIR, "..", "notebooks", "df_clean.csv")

DAY_LABELS = {
    0: "Mon", 1: "Tue", 2: "Wed",
    3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun",
}


@st.cache_data(show_spinner="Loading dataset...")
def load_data(path: str = DEFAULT_DATA_PATH) -> pd.DataFrame:
    """Load and enrich the cleaned delivery dataset."""
    df = pd.read_csv(
        path,
        parse_dates=["eater_request_timestamp_local"],
        low_memory=False,
    )

    df["hour"] = df["eater_request_timestamp_local"].dt.hour
    df["day_of_week"] = df["eater_request_timestamp_local"].dt.dayofweek
    df["day_label"] = df["day_of_week"].map(DAY_LABELS)
    df["total_distance"] = df["pickup_distance"] + df["dropoff_distance"]

    return df


def validate_data(df: pd.DataFrame) -> list[str]:
    """Return a list of validation warning messages (empty = no issues)."""
    warnings = []

    critical_cols = ["ATD", "pickup_distance", "dropoff_distance", "territory"]
    for col in critical_cols:
        if col not in df.columns:
            warnings.append(f"Missing expected column: `{col}`")
            continue
        null_count = int(df[col].isnull().sum())
        if null_count > 0:
            pct = null_count / len(df) * 100
            warnings.append(
                f"`{col}` has {null_count:,} null values ({pct:.1f}%)"
            )

    if "ATD" in df.columns:
        neg = int((df["ATD"] <= 0).sum())
        if neg > 0:
            warnings.append(
                f"`ATD` has {neg:,} non-positive values — data may be dirty"
            )

    return warnings


def filter_data(
    df: pd.DataFrame,
    territories: list[str],
    courier_flows: list[str],
    geo_archetypes: list[str],
    merchant_surfaces: list[str],
    hour_range: tuple[int, int],
) -> pd.DataFrame:
    """Apply sidebar filters and return the filtered DataFrame."""
    if not all([territories, courier_flows, geo_archetypes, merchant_surfaces]):
        return df.iloc[:0]  # Empty but schema-preserving

    mask = (
        df["territory"].isin(territories)
        & df["courier_flow"].isin(courier_flows)
        & df["geo_archetype"].isin(geo_archetypes)
        & df["merchant_surface"].isin(merchant_surfaces)
        & df["hour"].between(hour_range[0], hour_range[1])
    )
    return df[mask].copy()


def sample_for_scatter(df: pd.DataFrame, n: int = 5_000) -> pd.DataFrame:
    """Random sample for scatter plots to keep rendering fast."""
    if len(df) <= n:
        return df
    return df.sample(n=n, random_state=42)


def get_filter_options(df: pd.DataFrame) -> dict:
    """Extract sorted unique values for each sidebar filter."""
    return {
        "territories": sorted(df["territory"].dropna().unique().tolist()),
        "courier_flows": sorted(df["courier_flow"].dropna().unique().tolist()),
        "geo_archetypes": sorted(df["geo_archetype"].dropna().unique().tolist()),
        "merchant_surfaces": sorted(
            df["merchant_surface"].dropna().unique().tolist()
        ),
    }


def compute_kpis(df: pd.DataFrame) -> dict:
    """Compute top-level KPIs from the filtered dataset."""
    if df.empty:
        return {
            "avg_atd": None, "median_atd": None,
            "p90_atd": None, "n_trips": 0,
        }
    atd = df["ATD"].dropna()
    return {
        "avg_atd": float(np.mean(atd)),
        "median_atd": float(np.median(atd)),
        "p90_atd": float(np.percentile(atd, 90)),
        "n_trips": len(df),
    }
