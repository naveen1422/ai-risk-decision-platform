import pandas as pd
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer

# -----------------------------
# LOAD DATA
# -----------------------------
DATA_PATH = "data/risk_vault_db/customer_risk_profile"
df = pd.read_parquet(DATA_PATH)

# -----------------------------
# REMOVE DUPLICATES (IMPORTANT)
# -----------------------------
df = df.drop_duplicates(subset=["customer_id"])

# -----------------------------
# CLEAN TEXT CONVERSION
# -----------------------------
def row_to_text(row):
    flags = row["risk_flags"]

    if flags is not None:
        flags_list = list(flags)
        flags_clean = ", ".join(set(flags_list))
    else:
        flags_clean = "None"

    return f"""Customer {row['customer_id']}
Risk Score: {row['risk_score']}
Risk Bucket: {row['risk_bucket']}
Flags: {flags_clean}"""

# Create documents
documents = df.apply(row_to_text, axis=1).tolist()

# -----------------------------
# LOAD EMBEDDING MODEL
# -----------------------------
model = SentenceTransformer("all-MiniLM-L6-v2")

# Convert text → vectors
embeddings = model.encode(documents)

# -----------------------------
# CREATE VECTOR DB
# -----------------------------
dimension = embeddings.shape[1]
index = faiss.IndexFlatL2(dimension)
index.add(np.array(embeddings))

print("✅ Vector DB created")

# -----------------------------
# SEARCH FUNCTION (FINAL FIX)
# -----------------------------
def search(query, k=10):
    query_vector = model.encode([query])
    distances, indices = index.search(np.array(query_vector), k)

    seen_customers = set()
    results = []

    for idx in indices[0]:
        doc = documents[idx]

        # extract customer id (safe)
        lines = doc.split("\n")
        cust_id = lines[0].replace("Customer ", "").strip()

        if cust_id not in seen_customers:
            seen_customers.add(cust_id)
            results.append(doc)

        if len(results) == 3:
            break

    return results

# -----------------------------
# AI REASONING ENGINE
# -----------------------------
def generate_answer(query, retrieved_docs):
    response = f"\n🧠 AI Explanation System\n\n"
    response += f"Query: {query}\n\n"

    high = 0
    medium = 0
    low = 0

    response += "🔍 Retrieved Similar Customers:\n"

    for doc in retrieved_docs:
        response += f"\n{doc}\n"

        if "HIGH" in doc:
            high += 1
        elif "MEDIUM" in doc:
            medium += 1
        else:
            low += 1

    # -----------------------------
    # ANALYSIS
    # -----------------------------
    response += "\n🧠 Analysis:\n"

    if high >= 2:
        response += "- Majority of similar customers are HIGH risk.\n"
        response += "- Strong fraud/default patterns detected.\n"
    elif medium >= 2:
        response += "- Moderate risk behavior observed.\n"
    else:
        response += "- Mostly low risk behavior.\n"

    # -----------------------------
    # CONCLUSION
    # -----------------------------
    response += "\n📌 Conclusion:\n"

    if high >= 2:
        response += "This query indicates HIGH risk behavior."
    elif medium >= 2:
        response += "This query indicates MEDIUM risk behavior."
    else:
        response += "This query indicates LOW risk behavior."

    return response

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    query = input("Ask question: ")

    results = search(query)
    answer = generate_answer(query, results)

    print(answer)