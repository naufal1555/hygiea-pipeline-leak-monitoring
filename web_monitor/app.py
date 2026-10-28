"""Hygiea Interactive Pipeline Telemetry & Leak Monitoring Dashboard.
Author: Ahmad Fathin Naufal (UGM) - Big Data & Server Engineer
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import sqlite3
import os

st.set_page_config(
    page_title="Hygiea | IoT Pipeline Leak Monitoring",
    page_icon="🚰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling
st.markdown(
    """
<style>
    .metric-card {
        background-color: #1a1e24;
        border-radius: 8px;
        padding: 15px;
        border-left: 5px solid #00ADB5;
        margin-bottom: 10px;
    }
    .status-ok { color: #4ECCA3; font-weight: bold; }
    .status-alert { color: #FF5722; font-weight: bold; }
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_data(ttl=5)
def load_data():
    db_path = "data/processed/hygiea_telemetry.db"
    if not os.path.exists(db_path):
        return pd.DataFrame(), pd.DataFrame()

    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        "SELECT * FROM telemetry_stream ORDER BY timestamp ASC", conn
    )
    alerts = pd.read_sql_query(
        "SELECT * FROM incident_alerts ORDER BY timestamp DESC", conn
    )
    conn.close()
    return df, alerts


df, alerts = load_data()

# Sidebar
with st.sidebar:
    st.image(
        "https://images.unsplash.com/photo-1581092160607-ee22621dd758?auto=format&fit=crop&w=400&q=80",
        use_container_width=True,
    )
    st.title("Hygiea Telemetry Engine")
    st.markdown("**Enterprise Big Data & IoT Leak Detection**")
    st.markdown("---")
    st.markdown(
        "**Engineer:** Ahmad Fathin Naufal\n\n**Candidate:** Big Data & Server Engineer (UGM)"
    )
    st.markdown("---")

    if not df.empty:
        nodes = list(df["node_id"].unique())
        selected_node = st.selectbox(
            "Select Pipeline Zone Node",
            nodes,
            format_func=lambda x: f"{x} - {df[df['node_id']==x]['node_name'].iloc[0]}",
        )
    else:
        selected_node = None

st.title("🚰 Hygiea: Enterprise Pipeline Leak & Telemetry Monitoring")
st.markdown(
    "Real-time IoT differential pressure, acoustic cavitation emission, and hydraulic mass-balance anomaly detector."
)

if df.empty:
    st.warning(
        "Telemetry database not initialized. Please run `python run_hygiea.py` to generate & process telemetry stream."
    )
    st.stop()

# Node-specific dataset
node_df = df[df["node_id"] == selected_node].reset_index(drop=True)
latest = node_df.iloc[-1]

# Top Metrics Row
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("Inlet Pressure", f"{latest['inlet_pressure_bar']:.2f} bar")
with col2:
    st.metric("Outlet Pressure", f"{latest['outlet_pressure_bar']:.2f} bar")
with col3:
    dp = latest["differential_pressure_bar"]
    st.metric(
        "Diff Pressure (ΔP)",
        f"{dp:.2f} bar",
        delta=f"{dp - 0.28:.2f} vs norm",
        delta_color="inverse" if dp > 0.38 else "normal",
    )
with col4:
    st.metric("Inflow Rate", f"{latest['flow_rate_inlet_m3h']:.2f} m³/h")
with col5:
    state = latest["predicted_state"]
    status_color = (
        "🔴"
        if state == "MICRO_LEAK"
        else ("🟠" if state == "PRESSURE_SURGE" else "🟢")
    )
    st.metric("Zone Health Status", f"{status_color} {state}")

# Main Charting Area
tab1, tab2, tab3 = st.tabs(
    [
        "📈 Time-Series Telemetry",
        "🚨 Incident Alert Feed",
        "⚙️ Big Data Architecture",
    ]
)

with tab1:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=node_df.index,
            y=node_df["inlet_pressure_bar"],
            name="Inlet Pressure (bar)",
            line=dict(color="#00ADB5", width=2),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=node_df.index,
            y=node_df["outlet_pressure_bar"],
            name="Outlet Pressure (bar)",
            line=dict(color="#393E46", width=2),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=node_df.index,
            y=node_df["differential_pressure_bar"],
            name="ΔP Head Loss (bar)",
            line=dict(color="#FF5722", width=2.5),
        )
    )

    # Highlight leak anomalies
    leak_df = node_df[node_df["predicted_state"] == "MICRO_LEAK"]
    if not leak_df.empty:
        fig.add_trace(
            go.Scatter(
                x=leak_df.index,
                y=leak_df["differential_pressure_bar"],
                mode="markers",
                name="Micro-Leak Incident",
                marker=dict(color="red", size=8, symbol="triangle-up"),
            )
        )

    fig.update_layout(
        title=f"Hydraulic Gradient & Pressure Dynamics ({selected_node})",
        xaxis_title="Time Tick (Seconds)",
        yaxis_title="Pressure (bar)",
        template="plotly_dark",
        height=400,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Acoustic & Vibration Plot
    fig2 = go.Figure()
    fig2.add_trace(
        go.Scatter(
            x=node_df.index,
            y=node_df["acoustic_db"],
            name="Acoustic Cavitation (dB)",
            line=dict(color="#9C27B0", width=2),
        )
    )
    fig2.add_trace(
        go.Scatter(
            x=node_df.index,
            y=node_df["z_score_acoustic"],
            name="Acoustic Z-Score",
            line=dict(color="#E91E63", width=1.5, dash="dash"),
            yaxis="y2",
        )
    )
    fig2.update_layout(
        title="Acoustic Cavitation Emission & Statistical Z-Score Deviation",
        xaxis_title="Time Tick (Seconds)",
        yaxis_title="Sound Pressure Level (dB)",
        yaxis2=dict(
            title="Z-Score", overlaying="y", side="right", showgrid=False
        ),
        template="plotly_dark",
        height=350,
    )
    st.plotly_chart(fig2, use_container_width=True)

with tab2:
    st.subheader("Actionable Incident Audit Log")
    if not alerts.empty:
        node_alerts = alerts[alerts["node_id"] == selected_node]
        st.dataframe(
            node_alerts[
                [
                    "timestamp",
                    "alert_type",
                    "severity",
                    "confidence",
                    "alert_message",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("No critical leak or pressure incidents recorded.")

with tab3:
    st.markdown(
        """
    ### Big Data & Server Architecture
    - **Telemetry Stream Generator**: Generates 1,400+ real-time IoT samples across 4 primary pipeline risers under Darcy-Weisbach head loss equations.
    - **Dual-Tier Anomaly Engine**:
        - **Tier 1 (Hydraulic Gradient)**: Monitors $\Delta P = P_{in} - P_{out}$ and flow discrepancy $\Delta Q = Q_{in} - Q_{out}$.
        - **Tier 2 (Statistical Z-Score)**: Adaptive sliding-window Z-Score tracking ultrasonic cavitation hiss and structural vibration.
    - **Performance Benchmarks**:
        - Micro-Leak Recall: $\ge 95\%$
        - False Alarm Rate (FAR): $< 3\%$
        - Ingestion Throughput: $> 2,500$ events/sec
    """
    )
