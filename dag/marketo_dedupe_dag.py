from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from etl_utilities.utils import my_failure_callback


DAG_MD = """
# DAG Marketo_Dedupe

## Purpose

- This script is designed to merge duplicate records in Marketo based on a specific set of rules outlined on the Confluence page. 
It helps the Marketing team ensure there are no duplicate leads in Marketo for their campaigns.

Confluence Link: https://remindermediadevelopers.atlassian.net/wiki/spaces/BD/pages/1927315491/Marketo+Dedupe+Active

"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['business.intelligence@remindermedia.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': my_failure_callback
}


with DAG(
        'Marketo_Dedupe',
        default_args=default_args,
        schedule=None,
        start_date=datetime(2022, 7, 13),
        doc_md=DAG_MD,
        max_active_runs=1,
        max_active_tasks=1,
        catchup=False) as dag:

    t1 = BashOperator(
        task_id='Marketo_Dedupe_Task',
        bash_command='python3 /opt/airflow/etl_scripts/MarketoDedupe/marketo_dedupe.py',
    )
