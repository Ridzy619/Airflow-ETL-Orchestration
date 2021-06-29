from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    insert = """
    INSERT INTO {table}
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = self.insert.format(table=table) + sql
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        postgres_hook.run(self.sql)
        self.log.info('LoadFactOperator completed successfully.')
