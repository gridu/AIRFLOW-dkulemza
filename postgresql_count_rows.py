from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


class PostgresSQLCountRows(BaseOperator):

    @apply_defaults
    def __init__(self, table_name, *args, ** kwargs):
        """

        :param table_name: table name
        """
        self.table_name = table_name
        self.hook = PostgresHook(postgres_conn_id='airflow')
        super(PostgresSQLCountRows, self).__init__(*args, **kwargs)

    def execute(self, context):
        query = self.hook.get_first('SELECT COUNT(*) FROM {};'.format(self.table_name))
        return query


class MyPlugin(AirflowPlugin):
    name = 'custom_plugin'
    operators = [PostgresSQLCountRows]
