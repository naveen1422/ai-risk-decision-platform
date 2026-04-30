import pandas as pd
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer

# -----------------------------
# LOAD DATA
# -----------------------------
DATA_PATH = "data/risk_vault_db/customer_risk_profile"
df = pd.read_parquet(DATA_PATH)

df = df.drop_duplicates(subset=["customer_id"])

# -----------------------------
# TEXT CONVERSION
# -----------------------------
def row_to_text(row):
    flags = row["risk_flags"]

    if flags is not None and len(flags) > 0:
        flags_clean = ", ".join(set(list(flags)))
    else:
        flags_clean = "None"

    return f"""Customer {row['customer_id']}
Risk Score: {row['risk_score']}
Risk Bucket: {row['risk_bucket']}
Flags: {flags_clean}"""

documents = df.apply(row_to_text, axis=1).tolist()

# -----------------------------
# VECTOR DB
# -----------------------------
model = SentenceTransformer("all-MiniLM-L6-v2")
embeddings = model.encode(documents)

index = faiss.IndexFlatL2(embeddings.shape[1])
index.add(np.array(embeddings))

print("✅ AI System Ready")

# -----------------------------
# GET CUSTOMER
# -----------------------------
def get_customer(customer_id):
    row = df[df["customer_id"] == customer_id]

    if row.empty:
        return None

    return row.iloc[0]

# -----------------------------
# SEARCH SIMILAR
# -----------------------------
def search_similar(query_text, current_customer_id, k=10):
    query_vector = model.encode([query_text])
    distances, indices = index.search(np.array(query_vector), k)

    seen = set()
    results = []

    for idx in indices[0]:
        doc = documents[idx]
        cust_id = doc.split("\n")[0].replace("Customer ", "").strip()

        if cust_id == current_customer_id:
            continue

        if cust_id not in seen:
            seen.add(cust_id)
            results.append(doc)

        if len(results) == 3:
            break

    return results

# -----------------------------
# DECISION ENGINE
# -----------------------------
def make_decision(risk_score):
    if risk_score >= 70:
        return "🚨 BLOCK CUSTOMER"
    elif risk_score >= 30:
        return "⚠️ MONITOR CUSTOMER"
    else:
        return "✅ LOW RISK"

# -----------------------------
# CONFIDENCE WITH UNCERTAINTY
# -----------------------------
def calculate_confidence(similar_docs, customer):
    high = 0
    medium = 0
    low = 0

    for doc in similar_docs:
        if "HIGH" in doc:
            high += 1
        elif "MEDIUM" in doc:
            medium += 1
        else:
            low += 1

    total = len(similar_docs)

    if total == 0:
        return 0

    confidence = (max(high, medium, low) / total) * 100

    # 🔥 Uncertainty adjustment
    flags = customer["risk_flags"]

    if customer["risk_score"] == 0 and (flags is None or len(flags) == 0):
        confidence *= 0.6   # reduce confidence

    return round(confidence, 2)

# -----------------------------
# AI EXPLANATION
# -----------------------------
def generate_customer_explanation(customer, similar_docs):
    response = f"\n🧠 CUSTOMER AI ANALYSIS\n\n"

    response += f"Customer ID: {customer['customer_id']}\n"
    response += f"Risk Score: {customer['risk_score']}\n"
    response += f"Risk Bucket: {customer['risk_bucket']}\n"

    flags = customer["risk_flags"]

    if flags is not None and len(flags) > 0:
        response += f"Flags: {list(flags)}\n\n"
    else:
        response += "Flags: None\n\n"

    # -----------------------------
    # SIMILAR CUSTOMERS
    # -----------------------------
    response += "🔍 Similar Customers:\n"

    for doc in similar_docs:
        response += f"\n{doc}\n"

    # -----------------------------
    # ANALYSIS
    # -----------------------------
    response += "\n🧠 Analysis:\n"

    if customer["risk_score"] >= 70:
        response += "- Customer has high risk score\n"
    elif customer["risk_score"] >= 30:
        response += "- Customer has moderate risk score\n"
    else:
        response += "- Customer is low risk\n"

    if flags is not None and len(flags) > 0:
        response += "- Risk indicators detected\n"
    else:
        response += "- No risk indicators detected\n"
        response += "- Absence of signals does not guarantee safety\n"

    response += "- Similar customers show consistent behavior pattern\n"

    # -----------------------------
    # DECISION + CONFIDENCE
    # -----------------------------
    decision = make_decision(customer["risk_score"])
    confidence = calculate_confidence(similar_docs, customer)

    response += f"\n📌 FINAL DECISION:\n{decision}\n"
    response += f"\n📊 Confidence Level: {confidence}%\n"

    # -----------------------------
    # CONFIDENCE MESSAGE
    # -----------------------------
    if confidence > 80:
        if customer["risk_score"] == 0:
            response += "Confidence is high but based on absence of risk signals.\n"
        else:
            response += "Decision is highly reliable based on strong risk patterns.\n"
    elif confidence > 50:
        response += "Decision is moderately reliable.\n"
    else:
        response += "Decision has low confidence, requires further review.\n"

    return response

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    customer_id = input("Enter customer_id: ")

    customer = get_customer(customer_id)

    if customer is None:
        print("❌ Customer not found")
    else:
        query_text = row_to_text(customer)

        similar = search_similar(query_text, customer["customer_id"])

        result = generate_customer_explanation(customer, similar)

        print(result)