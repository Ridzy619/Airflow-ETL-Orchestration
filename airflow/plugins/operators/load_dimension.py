from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    insert = """
    INSERT INTO {table}
    """
    truncate = """
    TRUNCATE {table};
    """
#     on_conflict = """
#     ON CONFLICT ({col}) {expr}
#     """
#     do_update = """
#     DO UPDATE SET {col}=EXCLUDED.{col}
#     """
#     do_nothing = """DO NOTHING"""
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 append_data=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = self.insert.format(table=table) + sql
        if append_data is False:
            self.sql = self.truncate.format(table=table) + self.sql
            
    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        postgres_hook.run(self.sql)
        self.log.info('LoadDimensionOperator completed successfully.')
