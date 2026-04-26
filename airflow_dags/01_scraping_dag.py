from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

SCRIPTS = '/opt/airflow/airflow_scripts'

with DAG(
    dag_id='scraping_dag',
    default_args=default_args,
    description='Pipeline complet : Scraping -> ETL -> PostgreSQL',
    start_date=datetime(2026, 4, 1),
    schedule='@daily',
    catchup=False,
    tags=['scraping', 'etl', 'job-intelligent'],
) as dag:

    check = BashOperator(
        task_id='check_scripts_exist',
        bash_command=f'bash {SCRIPTS}/check.sh ',
    )

    scrape_adzuna = BashOperator(
        task_id='scrape_adzuna',
        bash_command=f'bash {SCRIPTS}/scrape_adzuna.sh ',
        execution_timeout=timedelta(minutes=15),
    )

    scrape_france = BashOperator(
        task_id='scrape_france_travail',
        bash_command=f'bash {SCRIPTS}/scrape_france.sh ',
        execution_timeout=timedelta(minutes=15),
    )

    scrape_wttj = BashOperator(
        task_id='scrape_welcometothejungle',
        bash_command=f'bash {SCRIPTS}/scrape_wttj.sh ',
        execution_timeout=timedelta(minutes=45),  # ← augmenté pour Selenium
    )

    etl_adzuna = BashOperator(
        task_id='etl_adzuna',
        bash_command=f'bash {SCRIPTS}/etl_adzuna.sh ',
        execution_timeout=timedelta(minutes=15),
    )

    etl_france = BashOperator(
        task_id='etl_france_travail',
        bash_command=f'bash {SCRIPTS}/etl_france.sh ',
        execution_timeout=timedelta(minutes=15),
    )

    etl_wttj = BashOperator(
        task_id='etl_welcomejungle',
        bash_command=f'bash {SCRIPTS}/etl_wttj.sh ',
        execution_timeout=timedelta(minutes=15),
    )

    load_pg = BashOperator(
        task_id='load_to_postgresql',
        bash_command=f'bash {SCRIPTS}/load_pg.sh ',
        execution_timeout=timedelta(minutes=20),
        trigger_rule=TriggerRule.ALL_DONE,  # ← continue même si wttj échoue
    )

    check >> [scrape_adzuna, scrape_france, scrape_wttj]
    scrape_adzuna >> etl_adzuna
    scrape_france >> etl_france
    scrape_wttj   >> etl_wttj
    [etl_adzuna, etl_france, etl_wttj] >> load_pg