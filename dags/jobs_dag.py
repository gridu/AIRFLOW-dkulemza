from datetime import datetime
import uuid

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook

from postgresql_count_rows import PostgresSQLCountRows


def push_xcom_call(**kwargs):
    kwargs['ti'].xcom_push(key='to_another_dag', value="{{ run_id }} ended")


def check_table_exist(sql_to_check_table_exist, name_of_table):
    """Check if table exists

    :param sql_to_check_table_exist: SQL script to check table exists
    :param name_of_table: table name
    :return: task name
    """
    hook = PostgresHook(postgres_conn_id='airflow')
    query = hook.get_first(sql=sql_to_check_table_exist.format(name_of_table))
    print(query)
    if query:
        return 'skip_table_creation'
    else:
        return 'create_table'


def print_message(dags_id, database):
    """Printing message

        :param dags_id: id of some dag
        :param database: name of database
        :return: message
        """
    return "{0} start processing tables in database: {1}". \
        format(dags_id, database)


def create_dag(dag_id, default_args):
    """Creating DAGs dynamically

    :param dag_id: id of some dag
    :param default_args: default arguments for dag
    :return:
    """

    dag = DAG(dag_id, default_args=default_args,
              schedule_interval='@once')

    with dag:
        message = PythonOperator(
            task_id='print_process_start',
            python_callable=print_message,
            op_args={dag.dag_id, 'airflow'},
        )
        user_name = BashOperator(
            task_id='user_name',
            bash_command='echo $USER',
            xcom_push=True,
        )

        branch_op = BranchPythonOperator(
            task_id='check_table_exists',
            python_callable=check_table_exist,
            op_args=["SELECT * FROM information_schema.tables "
                     "WHERE table_schema = 'public'"
                     "AND table_name = '{}';", 'my_user']
        )

        exists_table = DummyOperator(task_id='skip_table_creation')

        create_table = PostgresOperator(
            task_id='create_table',
            postgres_conn_id='airflow',
            sql='''CREATE TABLE my_user (custom_id INTEGER NOT NULL,
            user_name VARCHAR (300) NOT NULL, timestamp TIMESTAMP NOT NULL);'''
        )

        insert_row = PostgresOperator(
            task_id='insert_row',
            postgres_conn_id='airflow',
            trigger_rule=TriggerRule.ALL_DONE,
            sql='''INSERT INTO my_user VALUES
            (%s, %s, %s);''',
            parameters=(uuid.uuid4().
                        int % 123456789,
                        "SELECT * FROM "
                        "{{ task_instance.xcom_pull(task_ids='foo', "
                        "key='table_name') }};",
                        datetime.now())
        )

        query_row = PostgresSQLCountRows(
            task_id='query_row',
            postgres_conn_id='airflow',
            table_name='my_user',
            do_xcom_push=True,
            context=True,
            on_success_callback=push_xcom_call

        )

        message >> user_name >> branch_op
        branch_op >> [exists_table, create_table] >> insert_row
        insert_row >> query_row

    return dag


config = {
    'operation_with_database_1': {
        'schedule_interval': None,
        'start_date': datetime(2018, 11, 11),
        'task_name': ''
    },

    'operation_with_database_2': {
        'schedule_interval': None,
        'start_date': datetime(2018, 11, 11),
        'task_name': 'example_1'},

    'operation_with_database_3': {
        'schedule_interval': None,
        'start_date': datetime(2018, 11, 11),
        'task_name': 'example_1'},
}

for dag_name, conf in config.items():
    globals()[dag_name] = create_dag(dag_name, default_args=config[dag_name])
