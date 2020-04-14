from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable, XCom
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.timezone import make_aware


def pull_xcom_call(**kwargs):
    """Getting values from another DAG"""
    get_mane_xcoms_values__with_xcom_class = XCom.get_many(
        execution_date=make_aware(datetime(2018, 11, 11)),
        dag_ids=["operation_with_database_1"], include_prior_dates=True)
    print(get_mane_xcoms_values__with_xcom_class)

    get_xcom_with_ti = kwargs['ti'].xcom_pull(dag_id="operation_with_database_1", include_prior_dates=True)
    print(get_xcom_with_ti)


def load_subdag(parent_dag_name, child_dag_name, config_args, file_path):
    """Creating SubDag

    :param parent_dag_name: name of the parent dag
    :param child_dag_name: name of the sub dag
    :param config_args: configuration of dag
    :param file_path: path to the file 'run.txt'
    :return: dag
    """
    subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=config_args,
        schedule_interval='@once'
    )

    with subdag:
        external_sensor = ExternalTaskSensor(
            task_id='sensor_trigger_DAG',
            external_dag_id=parent_dag_name,
            external_task_id='trigger_dag',
            execution_delta=timedelta()
        )

        print_result = PythonOperator(
            task_id='print_result',
            python_callable=pull_xcom_call,
            provide_context=True
        )

        remove_file = BashOperator(
            task_id="remove_run_file",
            bash_command='rm {0}'.format(file_path)
        )

        timestamp = BashOperator(
            task_id='create_finished_timestamp',
            bash_command='echo {{ ts_nodash }}'
        )

        external_sensor >> print_result >> remove_file >> timestamp

    return subdag


config = {'schedule_interval': None, 'start_date': datetime(2018, 11, 11)}

default_path = '/Users/dkulemza/airflow/run.txt'

path = Variable.get('file_path') or default_path

with DAG('trigger_dag', default_args=config, schedule_interval='@once') as dag:
    files = FileSensor(
        task_id='check_file',
        filepath=path,
        poke_interval=30,
        soft_fail=True,
        dag=dag
    )

    trigger = TriggerDagRunOperator(
        task_id='trigger_dag',
        trigger_dag_id='trigger_dag',
        dag=dag
    )

    sub_dag = SubDagOperator(
        task_id='process_results_SubDag',
        subdag=load_subdag('trigger_dag',
                           'process_results_SubDag',
                           config, path),
        dag=dag
    )

    files >> trigger >> sub_dag
