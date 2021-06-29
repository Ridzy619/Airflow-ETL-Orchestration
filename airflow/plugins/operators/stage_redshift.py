from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('data_source',)
    
    sql_statement = """
        COPY {table_name}
        FROM '{data_source}'
        CREDENTIALS
        'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
        FORMAT AS JSON '{data_format}';
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 aws_bucket="",
                 aws_key="",
                 table_name="",
                 data_format="auto",
                 *args, **kwargs):
        

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.data_source = f"s3://{aws_bucket}/{aws_key}"
        self.table_name = table_name
        self.data_format = data_format
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.sql_statement = self.sql_statement.format(
            table_name=self.table_name,
            data_source=self.data_source,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            data_format=self.data_format
        )
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info(self.sql_statement)
        redshift_hook.run(self.sql_statement)
        self.log.info("StageToRedshiftOperator completed successfully.")