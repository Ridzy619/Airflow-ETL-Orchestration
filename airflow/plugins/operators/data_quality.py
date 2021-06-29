from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.dq_checks = dq_checks
            
    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        for check in self.dq_checks:
            check_sql = check.get("check_sql")
            expected_result = check.get("expected_result")
            
            records = postgres_hook.get_records(check_sql)
            self.log.info(records)
            
            if records[0][0] < expected_result:
                self.log.info("Data quality failed for script {}. {} expected, {} gotten.".format(check_sql, expected_result, records[0][0]))
                raise ValueError("Data quality failed for script {}.  {} expected, {} gotten.".format(check_sql, expected_result, records[0][0]))
            else:
                self.log.info("Data quality check passed for table {}".format(check_sql))
#     def __check_quality(self, table):
#         self.sql_statement = self.sql_statement.format(
#             self.table
#         )
#         records = postgres_hook.get_first(self.sql_statement)
        
#         if len(records)==0:
#             self.log.info("Data quality failed for table {}".format(self.table))
#             raise ValueError("Data quality failed for table {}".format(self.table))
            
#         if records[0][0]>0:
#             self.log.info("Data quality check passed for table {}".format(self.table))
#         else:
#             self.log.info("Data quality failed for table {}".format(self.table))
#             raise ValueError("Data quality failed for table {}".format(self.table))