from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor


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
                    '--py-files', 's3://ilgazy-udacity/scripts/utils.zip',
                    f's3://ilgazy-udacity/scripts/{script_name}.py',
                    '--config-path', 's3://ilgazy-udacity/scripts/config.yaml'
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
