import datetime
import json
import sqlite3

from customer_ai_system import (
    get_customer,
    row_to_text,
    search_similar,
    generate_customer_explanation,
    make_decision
)

# -----------------------------
# ACTION HANDLERS
# -----------------------------
def block_customer(customer_id):
    print(f"🚨 ACTION: Blocking customer {customer_id}")

def send_alert(customer_id):
    print(f"⚠️ ACTION: Alert sent for customer {customer_id}")

def log_customer(customer_id):
    print(f"📝 ACTION: Logged customer {customer_id} as low risk")


# -----------------------------
# LOGGING SYSTEM
# -----------------------------
def log_decision(
    customer_id,
    risk_score,
    risk_bucket,
    confidence,
    decision,
    explanation
):

    timestamp = str(datetime.datetime.now())

    # -----------------------------
    # JSON BACKUP LOG
    # -----------------------------
    log_entry = {
    "timestamp": str(timestamp),
    "customer_id": str(customer_id),
    "risk_score": int(risk_score),
    "risk_bucket": str(risk_bucket),
    "confidence": float(confidence),
    "decision": str(decision),
    "explanation": str(explanation)
    }

    with open("agent_log.json", "a") as f:
        f.write(json.dumps(log_entry) + "\n")

    # -----------------------------
    # SQLITE LOG
    # -----------------------------
    conn = sqlite3.connect("riskvault.db")

    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO risk_decisions (
            timestamp,
            customer_id,
            risk_score,
            risk_bucket,
            confidence,
            decision,
            explanation
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        str(timestamp),
    str(customer_id),
    int(risk_score),
    str(risk_bucket),
    float(confidence),
    str(decision),
    str(explanation)
    ))

    conn.commit()
    conn.close()


# -----------------------------
# AGENT ENGINE
# -----------------------------
def run_agent(customer_id):

    customer = get_customer(customer_id)

    if customer is None:
        print("❌ Customer not found")
        return

    risk_score = int(customer["risk_score"])
    risk_bucket = str(customer["risk_bucket"])

    query_text = row_to_text(customer)

    similar = search_similar(
        query_text,
        customer_id
    )

    explanation = generate_customer_explanation(
        customer,
        similar
    )

    print(explanation)

    decision = make_decision(risk_score)

    # -----------------------------
    # TEMP CONFIDENCE
    # -----------------------------
    confidence = 60.0

    # -----------------------------
    # ACTION BASED ON DECISION
    # -----------------------------
    if "BLOCK" in decision:

        block_customer(customer_id)

    elif "MONITOR" in decision:

        send_alert(customer_id)

    else:

        log_customer(customer_id)

    # -----------------------------
    # LOG DECISION
    # -----------------------------
    log_decision(
        customer_id,
        risk_score,
        risk_bucket,
        confidence,
        decision,
        explanation
    )

    print("✅ Agent execution completed")


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":

    customer_id = input("Enter customer_id: ")

    run_agent(customer_id)