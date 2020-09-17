from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

# references:   https://airflow-tutorial.readthedocs.io/en/latest/first-airflow.html
#               https://www.astronomer.io/guides/

default_args = {
            "owner": "airflow",
            "start_date": datetime(2019, 12, 4),
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "email": "youremail@host.com",
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        }

with DAG(dag_id="first_data_pipeline",
        schedule_interval="@daily",
        default_args=default_args,
        catchup=False) as dag:

    create_staging_table = PostgresOperator(
        task_id="create_staging_table",
        postgres_conn_id="postgres_employees",
        database="employees_db",
        sql='''
            CREATE TABLE IF NOT EXISTS staging.total_salary (sum_salary FLOAT);
            '''
    )

    delete_staging_table = PostgresOperator(
        task_id="delete_staging_table",
        postgres_conn_id="postgres_employees",
        database="employees_db",
        sql='''
            DELETE FROM staging.total_salary;
            '''
    )

    insert_staging_table = PostgresOperator(
        task_id="insert_staging_table",
        postgres_conn_id="postgres_employees",
        database="employees_db",
        sql='''
            INSERT INTO staging.total_salary(sum_salary) SELECT COUNT(*) as sum_salary FROM employees.salary
            '''
    )

    task_end = DummyOperator(
        task_id="task_end"
    )

    create_staging_table >> delete_staging_table >> insert_staging_table >> task_end