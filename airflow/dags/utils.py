import os

from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from omegaconf import OmegaConf

airflow_dir_path = "dags"
config = OmegaConf.load(os.path.join(airflow_dir_path, "config.yaml"))
config = OmegaConf.to_container(config, resolve=True)

file_schema = config.get('file_schema')
bucket_name = config.get('bucket_name')


def emr_step_task_group(script_name, cluster_id, aws_conn_id, dag):
    step = [
        {
            'Name': f'Run {script_name}.py',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'client',
                    '--py-files', f'{file_schema}{bucket_name}scripts/utils.zip',
                    f'{file_schema}{bucket_name}scripts/{script_name}.py',
                    '--config-path', f'{file_schema}{bucket_name}scripts/config.yaml'
                ]
            }
        }
    ]
    add_step = EmrAddStepsOperator(
        task_id='add_step',
        job_flow_id=cluster_id,
        aws_conn_id=aws_conn_id,
        steps=step,
        dag=dag
    )

    wait_step_completion = EmrStepSensor(
        task_id='wait_step_completion',
        job_flow_id=cluster_id,
        aws_conn_id=aws_conn_id,
        step_id=f"{{{{ ti.xcom_pull(task_ids='run_{script_name}.add_step')[0] }}}}",
        dag=dag
    )
    add_step.set_downstream(wait_step_completion)
    return add_step, wait_step_completion
