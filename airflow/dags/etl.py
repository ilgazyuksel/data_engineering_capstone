"""
## Capstone ETL
"""
import datetime
import os

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import (
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.utils.task_group import TaskGroup
from omegaconf import OmegaConf
from utils import emr_step_task_group

airflow_dir_path = "dags"
config = OmegaConf.load(os.path.join(airflow_dir_path, "config.yaml"))
config = OmegaConf.to_container(config, resolve=True)
aws_conn_id = config.get('aws_conn_id')
emr_conn_id = config.get('emr_conn_id')
mail_to = config.get('mail_to')
job_flow_overrides = config.get('job_flow_overrides')
cluster_id = "{{ ti.xcom_pull('create_cluster') }}"

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'email': mail_to,
    'email_on_failure': True,
    'execution_timeout': datetime.timedelta(minutes=90),
    'max_active_runs': 1
}
with DAG(
        'etl',
        default_args=default_args,
        description='ETL',
        catchup=False,
        start_date=datetime.datetime(2020, 1, 1),
        schedule_interval=None,
        tags=['udacity', 'etl'],
        default_view="graph",
) as dag:
    # generate dag documentation
    dag.doc_md = __doc__

    create_cluster = EmrCreateJobFlowOperator(
        dag=dag,
        task_id="create_cluster",
        job_flow_overrides=job_flow_overrides,
        aws_conn_id=aws_conn_id
    )
    wait_cluster_completion = EmrJobFlowSensor(
        task_id='wait_cluster_completion',
        job_flow_id=cluster_id,
        aws_conn_id=aws_conn_id,
        target_states=["RUNNING", "WAITING"],
        dag=dag
    )
    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster",
        trigger_rule="all_done",
        job_flow_id=cluster_id,
        aws_conn_id=aws_conn_id,
        dag=dag
    )

    with TaskGroup("run_country") as run_country:
        add_step, wait_step = emr_step_task_group(
            script_name='country', cluster_id=cluster_id, aws_conn_id=aws_conn_id, dag=dag
        )

    with TaskGroup("run_immigration") as run_immigration:
        add_step, wait_step = emr_step_task_group(
            script_name='immigration', cluster_id=cluster_id, aws_conn_id=aws_conn_id, dag=dag
        )

    with TaskGroup("run_global_temperatures") as run_global_temperatures:
        add_step, wait_step = emr_step_task_group(
            script_name='global_temperatures', cluster_id=cluster_id, aws_conn_id=aws_conn_id,
            dag=dag
        )

    with TaskGroup("run_us_cities_demographics") as run_us_cities_demographics:
        add_step, wait_step = emr_step_task_group(
            script_name='us_cities_demographics', cluster_id=cluster_id, aws_conn_id=aws_conn_id,
            dag=dag
        )

    with TaskGroup("run_gdp_per_capita") as run_gdp_per_capita:
        add_step, wait_step = emr_step_task_group(
            script_name='gdp_per_capita', cluster_id=cluster_id, aws_conn_id=aws_conn_id, dag=dag
        )

    with TaskGroup("run_human_capital_index") as run_human_capital_index:
        add_step, wait_step = emr_step_task_group(
            script_name='human_capital_index', cluster_id=cluster_id, aws_conn_id=aws_conn_id,
            dag=dag
        )

    with TaskGroup("run_press_freedom_index") as run_press_freedom_index:
        add_step, wait_step = emr_step_task_group(
            script_name='press_freedom_index', cluster_id=cluster_id, aws_conn_id=aws_conn_id,
            dag=dag
        )

    with TaskGroup("run_temperatures_by_country") as run_temperatures_by_country:
        add_step, wait_step = emr_step_task_group(
            script_name='temperatures_by_country', cluster_id=cluster_id, aws_conn_id=aws_conn_id,
            dag=dag
        )

    create_cluster >> wait_cluster_completion

    wait_cluster_completion >> [
        run_country,
        run_global_temperatures,
        run_immigration,
        run_us_cities_demographics
    ]

    run_country >> [
        run_gdp_per_capita,
        run_human_capital_index,
        run_press_freedom_index,
        run_temperatures_by_country
    ] >> terminate_cluster

    [
        run_global_temperatures,
        run_immigration,
        run_us_cities_demographics
    ] >> terminate_cluster
