import os
import pandas as pd

DATA_PATH = "data/risk_vault_db/customer_risk_profile"

def test_data_exists():
    assert os.path.exists(DATA_PATH), "Data path not found"

def test_data_load():
    df = pd.read_parquet(DATA_PATH)
    assert len(df) > 0, "Data is empty"

def test_required_columns():
    df = pd.read_parquet(DATA_PATH)
    required_cols = ["customer_id", "risk_score", "risk_bucket"]

    for col in required_cols:
        assert col in df.columns, f"Missing column: {col}"