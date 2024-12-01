from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule as tr
from datetime import datetime, timedelta
import random
import time

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1, 0, 0),
}

connection_name = "olympic"


def branch_dag(ti):
    medal = ti.xcom_pull(task_ids='choose_medal')
    return f"calc_{medal}"


with DAG(
        'olympic_medals_fin',
        default_args=default_args,
        schedule_interval='*/10 * * * *',
        catchup=False,
        tags=["artem_r"]
) as dag:
    create_table_task = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS artem_r;
        
        CREATE TABLE IF NOT EXISTS artem_r.olympic_medals (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(255),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    choose_medal_task = PythonOperator(
        task_id='choose_medal',
        python_callable=lambda: random.choice(['Bronze', 'Silver', 'Gold']),
        do_xcom_push=True
    )

    branch_task = BranchPythonOperator(
        task_id='branch',
        python_callable=branch_dag
    )

    bronze_task = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO artem_r.olympic_medals (medal_type, count) 
        SELECT 'Bronze', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Bronze'
        """
    )

    silver_task = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO artem_r.olympic_medals (medal_type, count) 
        SELECT 'Silver', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Silver'
        """
    )

    gold_task = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO artem_r.olympic_medals (medal_type, count) 
        SELECT 'Gold', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Gold'
        """
    )

    delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=lambda: time.sleep(5),
        trigger_rule=tr.ONE_SUCCESS
    )

    sensor_task = SqlSensor(
        task_id='check_for_correctness',
        conn_id=connection_name,
        sql="""
        SELECT MAX(created_at) > NOW() - INTERVAL 30 SECOND 
        FROM artem_r.olympic_medals;
    """,
        mode='poke',
        poke_interval=5,
        timeout=6,  # Maximum time to wait before failing
    )
    create_table_task >> choose_medal_task >> branch_task
    branch_task >> [bronze_task, silver_task, gold_task]
    [bronze_task, silver_task, gold_task] >> delay_task
    delay_task >> sensor_task
