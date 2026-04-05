"""Plotly visualization functions for the ATD dashboard."""
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from data import DAY_LABELS

_PALETTE = px.colors.qualitative.Plotly
_ATD_COLOR = "#636EFA"
_ERROR_COLOR = "#EF553B"
_ACCENT = "#00CC96"

_LAYOUT = dict(
    font=dict(family="Inter, Arial, sans-serif", size=13),
    paper_bgcolor="white",
    plot_bgcolor="#F9F9F9",
    margin=dict(t=40, b=40, l=50, r=20),
)


def _apply_layout(fig: go.Figure, **kwargs) -> go.Figure:
    layout = {**_LAYOUT, **kwargs}
    fig.update_layout(**layout)
    return fig


# ──────────────────────────────────────────────────────────────
# Overview
# ──────────────────────────────────────────────────────────────

def plot_atd_distribution_overview(df: pd.DataFrame) -> go.Figure:
    """Compact ATD histogram used in the Overview section."""
    fig = px.histogram(
        df,
        x="ATD",
        nbins=60,
        color_discrete_sequence=[_ATD_COLOR],
        labels={"ATD": "ATD (min)"},
        title="ATD Distribution",
    )
    fig.update_traces(marker_line_width=0.4, marker_line_color="white")
    return _apply_layout(fig, height=300)


# ──────────────────────────────────────────────────────────────
# Exploratory Analysis — Distributions
# ──────────────────────────────────────────────────────────────

def plot_atd_histogram(df: pd.DataFrame) -> go.Figure:
    """Full ATD histogram with percentile annotations."""
    p50 = df["ATD"].median()
    p90 = df["ATD"].quantile(0.9)

    fig = px.histogram(
        df,
        x="ATD",
        nbins=80,
        color_discrete_sequence=[_ATD_COLOR],
        labels={"ATD": "ATD (min)", "count": "Trips"},
        title="ATD Distribution",
        opacity=0.85,
    )
    fig.add_vline(
        x=p50, line_dash="dash", line_color="#FFA15A",
        annotation_text=f"P50: {p50:.1f} min",
        annotation_position="top right",
    )
    fig.add_vline(
        x=p90, line_dash="dash", line_color=_ERROR_COLOR,
        annotation_text=f"P90: {p90:.1f} min",
        annotation_position="top right",
    )
    fig.update_traces(marker_line_width=0.3, marker_line_color="white")
    return _apply_layout(fig, height=380)


def plot_atd_boxplot_by_territory(df: pd.DataFrame) -> go.Figure:
    """Boxplot of ATD for each territory, sorted by median ATD."""
    order = (
        df.groupby("territory")["ATD"]
        .median()
        .sort_values()
        .index.tolist()
    )
    fig = px.box(
        df,
        x="territory",
        y="ATD",
        category_orders={"territory": order},
        color="territory",
        color_discrete_sequence=_PALETTE,
        labels={"ATD": "ATD (min)", "territory": "Territory"},
        title="ATD by Territory",
        points=False,
    )
    fig.update_layout(showlegend=False, xaxis_tickangle=-30)
    return _apply_layout(fig, height=420)


# ──────────────────────────────────────────────────────────────
# Exploratory Analysis — Relationships
# ──────────────────────────────────────────────────────────────

def plot_atd_vs_distance(
    df: pd.DataFrame, distance_col: str, title: str
) -> go.Figure:
    """Scatter of ATD vs a distance column with a linear trend line."""
    fig = px.scatter(
        df,
        x=distance_col,
        y="ATD",
        opacity=0.35,
        color_discrete_sequence=[_ATD_COLOR],
        trendline="ols",
        trendline_color_override=_ERROR_COLOR,
        labels={
            distance_col: f"{distance_col.replace('_', ' ').title()} (km)",
            "ATD": "ATD (min)",
        },
        title=title,
    )
    return _apply_layout(fig, height=380)


# ──────────────────────────────────────────────────────────────
# Exploratory Analysis — Time
# ──────────────────────────────────────────────────────────────

def plot_avg_atd_by_hour(df: pd.DataFrame) -> go.Figure:
    """Bar chart of mean ATD for each hour of the day."""
    agg = (
        df.groupby("hour")["ATD"]
        .agg(avg_atd="mean", n="count")
        .reset_index()
    )
    fig = px.bar(
        agg,
        x="hour",
        y="avg_atd",
        color="avg_atd",
        color_continuous_scale="Blues",
        labels={"hour": "Hour of Day", "avg_atd": "Avg ATD (min)"},
        title="Average ATD by Hour of Day",
        text_auto=".1f",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(coloraxis_showscale=False, xaxis=dict(dtick=1))
    return _apply_layout(fig, height=380)


def plot_avg_atd_by_dow(df: pd.DataFrame) -> go.Figure:
    """Bar chart of mean ATD for each day of the week."""
    agg = (
        df.groupby(["day_of_week", "day_label"])["ATD"]
        .mean()
        .reset_index()
        .sort_values("day_of_week")
    )
    fig = px.bar(
        agg,
        x="day_label",
        y="ATD",
        color="ATD",
        color_continuous_scale="Blues",
        labels={"day_label": "Day of Week", "ATD": "Avg ATD (min)"},
        title="Average ATD by Day of Week",
        text_auto=".1f",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        coloraxis_showscale=False,
        xaxis=dict(
            categoryorder="array",
            categoryarray=[DAY_LABELS[i] for i in range(7)],
        ),
    )
    return _apply_layout(fig, height=340)


# ──────────────────────────────────────────────────────────────
# Performance Analysis
# ──────────────────────────────────────────────────────────────

def plot_actual_vs_predicted(
    y_true: np.ndarray, y_pred: np.ndarray
) -> go.Figure:
    """Scatter of actual vs predicted ATD with a perfect-prediction line."""
    sample_idx = np.random.default_rng(42).choice(
        len(y_true), size=min(5_000, len(y_true)), replace=False
    )
    yt = np.asarray(y_true)[sample_idx]
    yp = np.asarray(y_pred)[sample_idx]

    limit = max(yt.max(), yp.max()) * 1.05

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=yt, y=yp,
        mode="markers",
        marker=dict(color=_ATD_COLOR, opacity=0.4, size=4),
        name="Trips",
    ))
    fig.add_trace(go.Scatter(
        x=[0, limit], y=[0, limit],
        mode="lines",
        line=dict(color=_ERROR_COLOR, dash="dash", width=1.5),
        name="Perfect Prediction",
    ))
    fig.update_layout(
        title="Actual vs Predicted ATD",
        xaxis_title="Actual ATD (min)",
        yaxis_title="Predicted ATD (min)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    return _apply_layout(fig, height=420)


def plot_error_distribution(errors: np.ndarray) -> go.Figure:
    """Histogram of signed prediction errors (actual − predicted)."""
    fig = px.histogram(
        x=errors,
        nbins=80,
        color_discrete_sequence=[_ATD_COLOR],
        labels={"x": "Error (min)", "count": "Trips"},
        title="Error Distribution (Actual − Predicted)",
        opacity=0.85,
    )
    fig.add_vline(x=0, line_dash="dash", line_color=_ERROR_COLOR, line_width=1.5)
    fig.update_traces(marker_line_width=0.3, marker_line_color="white")
    return _apply_layout(fig, height=340)


def plot_abs_error_distribution(errors: np.ndarray) -> go.Figure:
    """Histogram of absolute prediction errors."""
    abs_errors = np.abs(errors)
    p50 = float(np.median(abs_errors))
    p90 = float(np.percentile(abs_errors, 90))

    fig = px.histogram(
        x=abs_errors,
        nbins=60,
        color_discrete_sequence=[_ACCENT],
        labels={"x": "Absolute Error (min)", "count": "Trips"},
        title="Absolute Error Distribution",
        opacity=0.85,
    )
    fig.add_vline(
        x=p50, line_dash="dash", line_color="#FFA15A",
        annotation_text=f"P50: {p50:.1f} min",
    )
    fig.add_vline(
        x=p90, line_dash="dash", line_color=_ERROR_COLOR,
        annotation_text=f"P90: {p90:.1f} min",
    )
    fig.update_traces(marker_line_width=0.3, marker_line_color="white")
    return _apply_layout(fig, height=340)


def plot_residuals(y_pred: np.ndarray, errors: np.ndarray) -> go.Figure:
    """Residual plot: predicted values vs signed errors."""
    sample_idx = np.random.default_rng(0).choice(
        len(y_pred), size=min(5_000, len(y_pred)), replace=False
    )
    yp = np.asarray(y_pred)[sample_idx]
    er = np.asarray(errors)[sample_idx]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=yp, y=er,
        mode="markers",
        marker=dict(color=_ATD_COLOR, opacity=0.4, size=4),
        name="Residuals",
    ))
    fig.add_hline(y=0, line_dash="dash", line_color=_ERROR_COLOR, line_width=1.5)
    fig.update_layout(
        title="Residual Plot",
        xaxis_title="Predicted ATD (min)",
        yaxis_title="Residual (min)",
    )
    return _apply_layout(fig, height=380)
