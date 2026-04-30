from kafka import KafkaConsumer
import json
import sys
import os

# import your agent function
sys.path.append(os.path.abspath("ai"))
from agent_system import run_agent

# -----------------------------
# KAFKA CONFIG
# -----------------------------
consumer = KafkaConsumer(
    'customer-events',   # topic name
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='agent-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🚀 Kafka Agent Consumer Started...")

# -----------------------------
# LISTEN TO EVENTS
# -----------------------------
for message in consumer:
    data = message.value

    print(f"\n📩 Received Event: {data}")

    customer_id = data.get("customer_id")

    if customer_id:
        print(f"⚡ Triggering AI Agent for {customer_id}")
        run_agent(customer_id)
    else:
        print("❌ Invalid message format")