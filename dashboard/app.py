"""
ATD Analytics Dashboard
========================
Streamlit app for analyzing and monitoring Actual Time of Delivery (ATD).

Run locally:
    cd delivery-time-analytics
    streamlit run dashboard/app.py
"""

import sys
import os

# Allow sibling imports when run directly via `streamlit run`
sys.path.insert(0, os.path.dirname(__file__))

import numpy as np
import streamlit as st

from charts import (
    plot_abs_error_distribution,
    plot_actual_vs_predicted,
    plot_atd_boxplot_by_territory,
    plot_atd_distribution_overview,
    plot_atd_histogram,
    plot_atd_vs_distance,
    plot_avg_atd_by_dow,
    plot_avg_atd_by_hour,
    plot_error_distribution,
    plot_residuals,
)
from data import (
    compute_kpis,
    filter_data,
    get_filter_options,
    load_data,
    sample_for_scatter,
    validate_data,
)
from models import (
    build_post_assign_features,
    build_pre_assign_features,
    compute_metrics,
    load_model_artifacts,
    load_pipeline,
    predict,
)

SECTIONS = [
    "Overview",
    "Exploratory Analysis",
    "Performance Analysis",
    "Model Predictions",
]


# ──────────────────────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────────────────────

def _configure_page() -> None:
    st.set_page_config(
        page_title="ATD Analytics Dashboard",
        page_icon="🚚",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.title("🚚 ATD Analytics Dashboard")
    st.caption(
        "Monitoring and analysis of Actual Time of Delivery across territories."
    )


# ──────────────────────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────────────────────

def _render_sidebar(df) -> tuple[str, object]:
    """Render navigation + filters. Returns (selected_section, filtered_df)."""
    with st.sidebar:
        st.header("Navigation")
        section = st.radio("Go to", SECTIONS, label_visibility="collapsed")

        st.divider()
        st.header("Filters")

        options = get_filter_options(df)

        territories = st.multiselect(
            "Territory",
            options=options["territories"],
            default=options["territories"],
        )
        courier_flows = st.multiselect(
            "Courier Flow",
            options=options["courier_flows"],
            default=options["courier_flows"],
        )
        geo_archetypes = st.multiselect(
            "Geo Archetype",
            options=options["geo_archetypes"],
            default=options["geo_archetypes"],
        )
        merchant_surfaces = st.multiselect(
            "Merchant Surface",
            options=options["merchant_surfaces"],
            default=options["merchant_surfaces"],
        )
        hour_range = st.slider(
            "Hour of Day", min_value=0, max_value=23, value=(0, 23)
        )

        st.divider()
        st.caption(f"Total trips loaded: {len(df):,}")

    df_filtered = filter_data(
        df, territories, courier_flows,
        geo_archetypes, merchant_surfaces, hour_range,
    )
    return section, df_filtered


# ──────────────────────────────────────────────────────────────
# Overview section
# ──────────────────────────────────────────────────────────────

def _render_overview(df: object) -> None:
    st.header("Overview")

    if df.empty:
        st.warning("No data matches the current filters.")
        return

    kpis = compute_kpis(df)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg ATD", f"{kpis['avg_atd']:.1f} min")
    col2.metric("Median ATD", f"{kpis['median_atd']:.1f} min")
    col3.metric("P90 ATD", f"{kpis['p90_atd']:.1f} min")
    col4.metric("Total Trips", f"{kpis['n_trips']:,}")

    st.subheader("ATD Distribution (filtered)")
    st.plotly_chart(
        plot_atd_distribution_overview(df),
        use_container_width=True,
    )

    st.subheader("Trips by Territory")
    territory_counts = (
        df["territory"]
        .value_counts()
        .reset_index()
        .rename(columns={"territory": "Territory", "count": "Trips"})
    )
    st.dataframe(
        territory_counts,
        use_container_width=True,
        hide_index=True,
    )


# ──────────────────────────────────────────────────────────────
# Exploratory Analysis section
# ──────────────────────────────────────────────────────────────

def _render_eda(df: object) -> None:
    st.header("Exploratory Analysis")

    if df.empty:
        st.warning("No data matches the current filters.")
        return

    # ── Distributions ────────────────────────────────────────
    st.subheader("Distributions")

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(plot_atd_histogram(df), use_container_width=True)
    with col2:
        st.plotly_chart(
            plot_atd_boxplot_by_territory(df), use_container_width=True
        )

    # ── Relationships ────────────────────────────────────────
    st.subheader("Relationships with Distance")
    st.caption(
        f"Showing a random sample of up to 5,000 trips for readability. "
        f"(Filtered total: {len(df):,})"
    )

    df_sample = sample_for_scatter(df)

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(
            plot_atd_vs_distance(
                df_sample, "dropoff_distance", "ATD vs Dropoff Distance"
            ),
            use_container_width=True,
        )
    with col2:
        st.plotly_chart(
            plot_atd_vs_distance(
                df_sample, "pickup_distance", "ATD vs Pickup Distance"
            ),
            use_container_width=True,
        )

    # ── Time Analysis ────────────────────────────────────────
    st.subheader("Time Patterns")

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(plot_avg_atd_by_hour(df), use_container_width=True)
    with col2:
        st.plotly_chart(plot_avg_atd_by_dow(df), use_container_width=True)


# ──────────────────────────────────────────────────────────────
# Performance Analysis section
# ──────────────────────────────────────────────────────────────

def _render_performance() -> None:
    st.header("Performance Analysis")
    st.info(
        "Model metrics are computed on the held-out test set "
        "(not affected by sidebar filters)."
    )

    variant = st.selectbox(
        "Model variant",
        options=["pre_assign", "post_assign"],
        format_func=lambda v: v.replace("_", "-").title(),
    )

    artifacts = load_model_artifacts(variant)
    if artifacts is None:
        st.warning(
            f"Model artifacts for **{variant}** not found in "
            f"`notebooks/`. Train the model first."
        )
        return

    model, X_test, y_test = artifacts

    # Filter NaN rows (can appear from log1p of negative dispatch_delay)
    nan_mask = ~np.isnan(X_test).any(axis=1)
    X_test = X_test[nan_mask]
    y_test = y_test[nan_mask]

    y_pred = model.predict(X_test)
    errors = y_test - y_pred

    # ── Metrics ──────────────────────────────────────────────
    st.subheader("Metrics")
    metrics = compute_metrics(y_test, y_pred)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("MAE", f"{metrics['MAE']:.2f} min")
    col2.metric("RMSE", f"{metrics['RMSE']:.2f} min")
    col3.metric("P50 Abs Error", f"{metrics['P50 Abs Error']:.2f} min")
    col4.metric("P90 Abs Error", f"{metrics['P90 Abs Error']:.2f} min")

    # ── Error plots ──────────────────────────────────────────
    st.subheader("Error Analysis")

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(
            plot_actual_vs_predicted(y_test, y_pred),
            use_container_width=True,
        )
    with col2:
        st.plotly_chart(
            plot_residuals(y_pred, errors),
            use_container_width=True,
        )

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(
            plot_error_distribution(errors),
            use_container_width=True,
        )
    with col2:
        st.plotly_chart(
            plot_abs_error_distribution(errors),
            use_container_width=True,
        )


# ──────────────────────────────────────────────────────────────
# Model Predictions section
# ──────────────────────────────────────────────────────────────

def _render_predictions(df: object) -> None:
    st.header("Model Predictions")
    st.write(
        "Enter trip details below to get an ATD prediction "
        "from the trained model."
    )

    variant = st.selectbox(
        "Model variant",
        options=["pre_assign", "post_assign"],
        format_func=lambda v: v.replace("_", "-").title(),
        key="pred_variant",
    )

    pipeline = load_pipeline(variant)
    artifacts = load_model_artifacts(variant)

    if pipeline is None or artifacts is None:
        st.warning(
            f"Artifacts for **{variant}** not found in `notebooks/`. "
            "Train the model first."
        )
        return

    model, _, _ = artifacts
    options = get_filter_options(df)

    with st.form("prediction_form"):
        st.subheader("Trip Details")

        col1, col2, col3 = st.columns(3)
        with col1:
            pickup_dist = st.number_input(
                "Pickup Distance (km)", min_value=0.0,
                max_value=50.0, value=2.0, step=0.1,
            )
            dropoff_dist = st.number_input(
                "Dropoff Distance (km)", min_value=0.0,
                max_value=50.0, value=3.0, step=0.1,
            )
            hour = st.slider("Hour of Day", min_value=0, max_value=23, value=12)
        with col2:
            territory = st.selectbox("Territory", options=options["territories"])
            courier_flow = st.selectbox(
                "Courier Flow", options=options["courier_flows"]
            )
        with col3:
            geo_archetype = st.selectbox(
                "Geo Archetype", options=options["geo_archetypes"]
            )
            merchant_surface = st.selectbox(
                "Merchant Surface", options=options["merchant_surfaces"]
            )

        if variant == "post_assign":
            st.subheader("Post-Assignment Details")
            col1, col2 = st.columns(2)
            with col1:
                dispatch_delay = st.number_input(
                    "Dispatch Delay (min)", min_value=0.0,
                    max_value=120.0, value=5.0, step=0.5,
                )
            with col2:
                offered_equals_request = st.selectbox(
                    "Offered = Request Timestamp?",
                    options=[0, 1],
                    format_func=lambda x: "Yes" if x else "No",
                )

        submitted = st.form_submit_button("Predict ATD", type="primary")

    if submitted:
        try:
            if variant == "pre_assign":
                features = build_pre_assign_features(
                    pickup_dist, dropoff_dist,
                    territory, courier_flow, geo_archetype, merchant_surface,
                    hour,
                )
            else:
                features = build_post_assign_features(
                    pickup_dist, dropoff_dist,
                    territory, courier_flow, geo_archetype, merchant_surface,
                    hour, dispatch_delay, offered_equals_request,
                )

            prediction = predict(pipeline, model, features)

            st.success(f"**Predicted ATD: {prediction:.1f} minutes**")
            st.caption(
                f"Model: {variant.replace('_', '-').title()} | "
                f"Distance: {pickup_dist + dropoff_dist:.1f} km total"
            )
        except Exception as e:
            st.error(f"Prediction failed: {e}")


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────

def main() -> None:
    _configure_page()

    df = load_data()

    warnings = validate_data(df)
    if warnings:
        with st.expander("⚠ Data quality warnings", expanded=False):
            for w in warnings:
                st.warning(w)

    section, df_filtered = _render_sidebar(df)

    if section == "Overview":
        _render_overview(df_filtered)
    elif section == "Exploratory Analysis":
        _render_eda(df_filtered)
    elif section == "Performance Analysis":
        _render_performance()
    elif section == "Model Predictions":
        _render_predictions(df)


if __name__ == "__main__":
    main()
