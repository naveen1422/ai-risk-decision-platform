from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="risk_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    layer2 = BashOperator(
        task_id="run_gold_layer",
        bash_command="python /opt/project/spark_jobs/gold/layer2_risk/build_customer_risk.py"
    )

    layer3 = BashOperator(
        task_id="run_gold_layer3",
        bash_command="python /opt/project/spark_jobs/gold/layer3_insights/build_customer_insights.py"
    )

    layer2 >> layer3