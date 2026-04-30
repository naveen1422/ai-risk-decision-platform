import datetime
import json
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
def log_decision(customer_id, decision, explanation):
    timestamp = str(datetime.datetime.now())

    log_entry = {
        "timestamp": timestamp,
        "customer_id": customer_id,
        "decision": decision,
        "explanation": explanation
    }

    with open("agent_log.json", "a") as f:
        f.write(json.dumps(log_entry) + "\n")

# -----------------------------
# AGENT ENGINE
# -----------------------------
def run_agent(customer_id):
    customer = get_customer(customer_id)

    if customer is None:
        print("❌ Customer not found")
        return

    query_text = row_to_text(customer)

    similar = search_similar(query_text, customer_id)

    explanation = generate_customer_explanation(customer, similar)

    print(explanation)

    decision = make_decision(customer["risk_score"])

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
    # LOG EVERY ACTION
    # -----------------------------
    log_decision(customer_id, decision, explanation)

    print("✅ Agent execution completed")

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    customer_id = input("Enter customer_id: ")
    run_agent(customer_id)