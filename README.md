AI Risk Decision Platform

## 📌 Overview
. This project is a real-time AI-driven risk decision system designed to simulate how modern financial platforms detect and respond to risky customer behavior.

. This system reflects a real-world principle:
  1) Data → Intelligence → Decision → Action → Observability
  2)Built as a production-style AI system focusing on system design, scaling

. This project builds an end-to-end data pipeline to identify risky/defaulter customers using behavioral patterns, transaction analysis, and network relationships.
It provides:
- Risk scoring (1–200 scale)
- Behavioral insights
- Network-based risk signals
- AI-based explanation for decisions

. It combines data engineering, streaming, AI reasoning, and system design into a unified platform that:
1) Processes incoming events in real time
2) Applies intelligent risk analysis
3) Makes automated decisions
4) Executes actions via an agent
5) Provides live monitoring through a dashboard


## 🏗️ Architecture
Kafka Event
↓
Consumer
↓
Bronze Layer → Raw Data (transactions, accounts)
↓
Silver Layer → Cleaned & structured data
↓
Gold Layer → Features + Risk scoring
↓
AI Engine→ Decision → Agent → JSON Log → Streamlit Dashboard

⚙️ Key Components
1️⃣ Data Platform (Batch Layer)
Bronze → Silver → Gold architecture
Built using Spark-style transformations
Gold layer produces customer_risk_profile
2️⃣ Streaming Layer (Real-Time)
Kafka used for event ingestion
Producer sends customer events
Consumer triggers AI system instantly
3️⃣ AI Decision Engine
Rule-based + RAG (Retrieval-Augmented) system
Uses vector similarity to compare customers
Generates:
Risk score
Risk bucket
Risk flags
Explanation
4️⃣ Agent System (Action Layer)
Based on decision:
HIGH   → Block customer
MEDIUM → Send alert
LOW    → Log & monitor
Includes:
Action execution
Logging
Audit trail
5️⃣ Real-Time Dashboard
Built using Streamlit:
Live Kafka event monitoring
Decision visualization
Risk distribution charts
Latest AI explanation display
6️⃣ CI/CD Pipeline
Implemented using GitHub Actions
Automated:
Dependency installation
Data validation tests
Pipeline checks
7️⃣ Environment Management
Supports:
dev → development
uat → validation
prod → production
Using .env configurations:
Separate logs
Separate topics
Controlled deployments

🧰 Tech Stack
🔹 Data Engineering
Python
Pandas
Parquet
🔹 Streaming
Kafka
🔹 AI / ML
Sentence Transformers
FAISS (Vector DB)
RAG architecture
🔹 Orchestration
Airflow
🔹 UI
Streamlit
🔹 DevOps
GitHub Actions (CI/CD)
Docker (for deployment)
🔹 Cloud (in progress)
GCP Cloud Run
Cloud Storage (planned)
🔥 Key Features
✔ Real-time event-driven architecture
✔ AI-powered risk decisioning
✔ Explainable AI outputs
✔ Autonomous agent execution
✔ Live monitoring dashboard
✔ CI/CD integration
✔ Environment separation
✔ Cloud-ready design
🎯 What This Project Demonstrates

This project showcases:
1) End-to-end data platform design
2) Event-driven system architecture
3) AI + data engineering integration
4) Decision intelligence systems
5) Production-style thinking (CI/CD, environments, logging)
6) Future Enhancements
7) Replace rule-based logic with ML model
8) Integrate LLM for advanced explanations
9) Move Kafka → GCP Pub/Sub
10) Store logs in BigQuery / Cloud Storage
11) Add alerting system
12)  to high-volume streaming
13) Key Insight
14) ability, and real-world applicability.

## 🖥️ UI Screenshots


Example:
- Customer Risk Summary
- Transaction Behavior
  
## Features
- Rule-based risk scoring system
- Behavioral pattern detection
- Network (counterparty) analysis
- Real-time risk evaluation ready
- Scalable pipeline (Spark + Airflow)
- AI-based explanation layer

## 🚧 Future Work
- Add Machine Learning model (risk prediction)
- Advanced graph/network analytics
- Production deployment (AWS/GCP)
- Role-based UI dashboard

## 📂 Project Structure
defaulter-platform/
├── spark_jobs/
├── core/
├── ai/
├── airflow/
├── tests/
├── ui/
├── requirements.txt

## 👤 Author

Naveen Chandra


