import streamlit as st
import json
import time
import pandas as pd

st.set_page_config(page_title="AI Risk Dashboard", layout="wide")

st.title("🚀 Real-Time AI Risk Dashboard")

# -----------------------------
# AUTO REFRESH CONTROL
# -----------------------------
refresh_rate = st.slider("Refresh every seconds", 1, 10, 3)

placeholder = st.empty()

while True:
    data = []

    try:
        with open("agent_log.json", "r") as f:
            for line in f:
                data.append(json.loads(line))
    except:
        pass

    if data:
        df = pd.DataFrame(data[::-1])  # latest first

        with placeholder.container():

            st.subheader("📡 Live Kafka Events")

            st.dataframe(df[["timestamp", "customer_id", "decision"]])

            st.subheader("📊 Decision Summary")

            decision_counts = df["decision"].value_counts()
            st.bar_chart(decision_counts)

            st.subheader("🧠 Latest Analysis")

            latest = df.iloc[0]

            st.write(f"Customer: {latest['customer_id']}")
            st.write(f"Decision: {latest['decision']}")
            st.text(latest["explanation"])

    else:
        st.info("Waiting for Kafka events...")

    time.sleep(refresh_rate)