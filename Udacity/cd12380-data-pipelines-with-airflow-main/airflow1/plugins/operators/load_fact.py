from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        self.log.info(f'LoadFactOperator starting for table {self.table}')
        
        # Get Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Build INSERT statement
        insert_sql = f"INSERT INTO {self.table} {self.sql}"
        
        self.log.info(
            f"Executing INSERT statement for fact table {self.table}"
        )
        redshift.run(insert_sql)
        
        self.log.info(f'LoadFactOperator completed for table {self.table}')
