import sqlite3

conn = sqlite3.connect("riskvault.db")

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS risk_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    customer_id TEXT,
    risk_score INTEGER,
    risk_bucket TEXT,
    confidence REAL,
    decision TEXT,
    explanation TEXT
)
""")

conn.commit()
conn.close()

print("✅ Database Created")