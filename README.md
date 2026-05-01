AI Risk Decision Platform
Overview

This project is a real-time AI-driven risk decision system designed to simulate how modern financial platforms detect and respond to risky customer behavior.

## 📌 Overview
This project builds an end-to-end data pipeline to identify risky/defaulter customers using behavioral patterns, transaction analysis, and network relationships.

It provides:
- Risk scoring (1–200 scale)
- Behavioral insights
- Network-based risk signals
- AI-based explanation for decisions


It combines data engineering, streaming, AI reasoning, and system design into a unified platform that:

Processes incoming events in real time
Applies intelligent risk analysis
Makes automated decisions
Executes actions via an agent
Provides live monitoring through a dashboard
🏗️ System Architecture
Kafka Event → Consumer → AI Engine → Decision → Agent → JSON Log → Streamlit Dashboard
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

End-to-end data platform design
Event-driven system architecture
AI + data engineering integration
Decision intelligence systems
Production-style thinking (CI/CD, environments, logging)
Future Enhancements
Replace rule-based logic with ML model
Integrate LLM for advanced explanations
Move Kafka → GCP Pub/Sub
Store logs in BigQuery / Cloud Storage
Add alerting system
Scale to high-volume streaming
Key Insight

This system reflects a real-world principle:

Data → Intelligence → Decision → Action → Observability


Built as a production-style AI system focusing on system design, scalability, and real-world applicability.

## 🏗️ Architecture
Bronze Layer → Raw Data (transactions, accounts)
↓
Silver Layer → Cleaned & structured data
↓
Gold Layer → Features + Risk scoring
↓
UI + AI → Insights + Explanation

## 🛠️ Tech Stack

- Python
- PySpark
- Airflow
- Delta / Parquet
- Streamlit (UI)
- OpenAI (AI explanation)
- Git + GitHub

## 🛠️ Tech Stack

- Python
- PySpark
- Airflow
- Delta / Parquet
- Streamlit (UI)
- OpenAI (AI explanation)
- Git + GitHub

## 🖥️ UI Screenshots

*(Add screenshots later)*

Example:
- Customer Risk Summary
- Transaction Behavior
- Network Analysis

---

## Features

- Rule-based risk scoring system
- Behavioral pattern detection
- Network (counterparty) analysis
- Real-time risk evaluation ready
- Scalable pipeline (Spark + Airflow)
- AI-based explanation layer

## 🚧 Future Work

- Add Machine Learning model (risk prediction)
- Real-time streaming (Kafka)
- Advanced graph/network analytics
- Production deployment (AWS/GCP)
- Role-based UI dashboard

---

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


