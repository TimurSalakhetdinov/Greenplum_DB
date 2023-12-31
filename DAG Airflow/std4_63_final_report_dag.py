from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

# Constants and configurations
DB_CONN = "gp_sapiens_std4_63"
DB_SCHEMA = 'std4_63'

default_args = {
    'owner': 'std4_63',
    'start_date': datetime(2021, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}

# Function to calculate the date range for the data mart
def get_report_dates(execution_date):
    # Start date is static for the entire reporting period
    start_date = '20210101'  
    # Calculate the end date as the end of the current month
    end_of_month = execution_date.replace(day=1) + timedelta(days=32)
    end_of_month = end_of_month - timedelta(days=end_of_month.day)
    end_date = end_of_month.strftime('%Y%m%d')
    return start_date, end_date

# DAG definition
with DAG(
    "std4_63_main_dag",
    default_args=default_args,
    schedule_interval="0 0 1 * *",  # Runs at the start of each month
    catchup=False
) as dag:

    # Start and end markers for the DAG
    start_task = DummyOperator(task_id="start")
    end_task = DummyOperator(task_id="end")

    # Load reference data task group
    with TaskGroup("load_reference_data") as load_reference_data:
        tables = ['stores', 'coupons', 'promos', 'promo_types']
        for table in tables:
            PostgresOperator(
                task_id=f"load_full_{table}",
                postgres_conn_id=DB_CONN,
                sql=f"SELECT {DB_SCHEMA}.f_load_full('{DB_SCHEMA}.{table}', '{table}_file');"
            )

    # Load fact tables task group with the specified date columns
    with TaskGroup("load_fact_tables") as load_fact_tables:
        fact_tables = {
            'traffic': 'date',
            'bills_head': 'calday',
            'bills_item': 'calday',
        }
        for table, date_column in fact_tables.items():
            PostgresOperator(
                task_id=f"load_{table}",
                postgres_conn_id=DB_CONN,
                sql=f"""
                SELECT {DB_SCHEMA}.f_load_pxf(
                    '{DB_SCHEMA}.{table}',
                    'gp.{table}',
                    '{date_column}',  -- using the specific date column for each table
                    '{{{{ prev_ds }}}}',  -- start of the previous month
                    '{{{{ ds }}}}',      -- start of the current month
                    'intern', 'intern'
                );
                """
            )

    # Calculate the data mart
    calculate_mart = PostgresOperator(
        task_id="calculate_data_mart",
        postgres_conn_id=DB_CONN,
        sql="""
        SELECT {DB_SCHEMA}.f_report_data_mart(
            '{{ params.start_date }}',
            '{{ params.end_date }}'
        );
        """.format(DB_SCHEMA=DB_SCHEMA),
        params={'start_date': '{{ get_report_dates(ds)[0] }}',
                'end_date': '{{ get_report_dates(ds)[1] }}'},
    )

    # Define workflow
    start_task >> load_reference_data >> load_fact_tables >> calculate_mart >> end_task
