# orchestration/dags/dq_telemetry_checks.py
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from pendulum import datetime

# --- Configuration ---
CRITICAL_TEMP_THRESHOLD = 148.0
def run_temperature_quality_check():
    """Queries the live Gold Layer to check for thermal anomalies."""
    print("Initiating Telemetry Data Quality Scan...")
    
    pg_hook = PostgresHook(postgres_conn_id='permutex_pg_conn')
    
    sql = f"""
        SELECT satellite_id, window_end, max_temperature 
        FROM satellite_telemetry_gold
        WHERE max_temperature > {CRITICAL_TEMP_THRESHOLD}
        ORDER BY window_end DESC
        LIMIT 5;
    """
    
    records = pg_hook.get_records(sql)
    
    if records:
        print(f"CRITICAL ALERT: {len(records)} thermal anomalies detected!")
        for row in records:
            print(f" - Satellite {row[0]} recorded {row[2]}°C at {row[1]}")
        
        raise ValueError(f"Data Quality Failure: {len(records)} anomalies found.")
    else:
        print("Data Quality Check Passed: All satellite temperatures are nominal.")

with DAG(
    dag_id="telemetry_quality_monitor",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *", # Run every 5 minutes
    catchup=False,
    tags=["permutex", "data-quality"],
) as dag:

    dq_check_task = PythonOperator(
        task_id="check_thermal_anomalies",
        python_callable=run_temperature_quality_check
    )