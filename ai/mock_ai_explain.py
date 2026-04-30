import pandas as pd

# -----------------------------
# LOAD DATA
# -----------------------------
DATA_PATH = "data/risk_vault_db/customer_risk_profile"

df = pd.read_parquet(DATA_PATH)

# -----------------------------
# GET CUSTOMER DATA
# -----------------------------
def get_customer(customer_id):
    filtered = df[df["customer_id"] == customer_id]

    if filtered.empty:
        return None

    return filtered.iloc[0]

# -----------------------------
# MOCK AI EXPLANATION ENGINE
# -----------------------------
def generate_explanation(row):
    risk_score = row["risk_score"]
    risk_bucket = row["risk_bucket"]
    flags = row["risk_flags"]

    explanation = f"\n🧠 AI Explanation:\n\n"
    explanation += f"Customer {row['customer_id']} is classified as {risk_bucket} risk.\n\n"

    # Risk score reason
    explanation += f"- Risk score is {risk_score}, which falls under {risk_bucket} category.\n"

    # Handle flags safely
    if flags is not None and len(flags) > 0:
        explanation += "- Key risk indicators detected:\n"

        for flag in flags:
            explanation += f"  • {flag}\n"

    # Additional logic
    if risk_score >= 70:
        explanation += "\nThis customer requires immediate attention.\n"
    elif risk_score >= 30:
        explanation += "\nThis customer should be monitored closely.\n"
    else:
        explanation += "\nThis customer is considered low risk.\n"

    return explanation

# -----------------------------
# MAIN RUN
# -----------------------------
if __name__ == "__main__":
    customer_id = input("Enter customer_id: ")

    row = get_customer(customer_id)

    if row is None:
        print("❌ Customer not found")
    else:
        result = generate_explanation(row)
        print(result)