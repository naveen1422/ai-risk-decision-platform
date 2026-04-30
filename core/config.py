import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_PATH = os.path.join(PROJECT_ROOT, "data")

CUSTOMER_DB_PATH = os.path.join(DATA_PATH, "customer_db")
ACCOUNT_DB_PATH = os.path.join(DATA_PATH, "account_db")
LOAN_DB_PATH = os.path.join(DATA_PATH, "loan_db")
PAYMENT_DB_PATH = os.path.join(DATA_PATH, "payment_db")
TRANSACTION_DB_PATH = os.path.join(DATA_PATH, "transaction_db")