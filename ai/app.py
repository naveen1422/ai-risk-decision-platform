import streamlit as st
import pandas as pd
import sqlite3
from streamlit_autorefresh import st_autorefresh

# -----------------------------
# PAGE CONFIG
# -----------------------------
st.set_page_config(
    page_title="AI Risk Dashboard",
    layout="wide"
)

st.title("🚀 Real-Time AI Risk Dashboard")

# -----------------------------
# REFRESH CONTROL
# -----------------------------
refresh_rate = st.slider(
    "Refresh every seconds",
    1,
    10,
    3
)

# -----------------------------
# AUTO REFRESH
# -----------------------------
st_autorefresh(
    interval=refresh_rate * 1000,
    key="riskvault_refresh"
)

# -----------------------------
# LOAD DATA FROM SQLITE
# -----------------------------
try:

    conn = sqlite3.connect("riskvault.db")

    df = pd.read_sql("""
        SELECT *
        FROM risk_decisions
        ORDER BY timestamp DESC
    """, conn)

    conn.close()

except Exception as e:

    st.error(f"Database Error: {e}")

    df = pd.DataFrame()

# -----------------------------
# DASHBOARD
# -----------------------------
if not df.empty:

    st.subheader("📡 Live Kafka Events")

    st.dataframe(
        df[
            [
                "timestamp",
                "customer_id",
                "risk_score",
                "risk_bucket",
                "confidence",
                "decision"
            ]
        ],
        use_container_width=True
    )

    # -----------------------------
    # DECISION SUMMARY
    # -----------------------------
    st.subheader("📊 Decision Summary")

    decision_counts = df["decision"].value_counts()

    st.bar_chart(decision_counts)

    # -----------------------------
    # RISK BUCKET SUMMARY
    # -----------------------------
    st.subheader("⚠️ Risk Bucket Distribution")

    risk_counts = df["risk_bucket"].value_counts()

    st.bar_chart(risk_counts)

    # -----------------------------
    # LATEST AI EXPLANATION
    # -----------------------------
    st.subheader("🧠 Latest AI Analysis")

    st.text_area(
        "Explanation",
        value=df.iloc[0]["explanation"],
        height=350
    )

    # -----------------------------
    # METRICS
    # -----------------------------
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Total Decisions",
            len(df)
        )

    with col2:
        st.metric(
            "Average Confidence",
            f"{df['confidence'].mean():.1f}%"
        )

    with col3:
        st.metric(
            "Unique Customers",
            df["customer_id"].nunique()
        )

else:

    st.warning("No risk decisions available yet.")

# -----------------------------
# FOOTER
# -----------------------------
st.caption(
    f"Dashboard auto-refreshes every {refresh_rate} seconds"
)