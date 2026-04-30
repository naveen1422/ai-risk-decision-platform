import pandas as pd
from openai import OpenAI

# -----------------------------
# CONFIG
# -----------------------------
client = OpenAI(api_key="YOUR_OPENAI_API_KEY")

DATA_PATH = "data/risk_vault_db/customer_risk_profile"

# -----------------------------
# LOAD DATA
# -----------------------------
df = pd.read_parquet(DATA_PATH)

# -----------------------------
# FUNCTION: GET CUSTOMER DATA
# -----------------------------
def get_customer_data(customer_id):
    row = df[df["customer_id"] == customer_id].iloc[0]
    return row

# -----------------------------
# FUNCTION: BUILD CONTEXT
# -----------------------------
def build_context(row):
    return f"""
    Customer ID: {row['customer_id']}
    Risk Score: {row['risk_score']}
    Risk Bucket: {row['risk_bucket']}
    Risk Flags: {row['risk_flags']}
    """

# -----------------------------
# FUNCTION: ASK LLM
# -----------------------------
def explain_risk(context):
    prompt = f"""
    You are a risk analyst.

    Based on the following customer data, explain clearly why the customer is high or low risk.

    {context}

    Give a simple explanation.
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}]
    )

    return response.choices[0].message.content

# -----------------------------
# RUN
# -----------------------------
customer_id = input("Enter customer_id: ")

row = get_customer_data(customer_id)
context = build_context(row)

explanation = explain_risk(context)

print("\n🧠 AI Explanation:\n")
print(explanation)