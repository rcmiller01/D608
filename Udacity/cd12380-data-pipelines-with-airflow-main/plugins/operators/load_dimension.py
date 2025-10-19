from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 mode="truncate-insert",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.mode = mode

    def execute(self, context):
        self.log.info(
            f'LoadDimensionOperator starting for table {self.table} '
            f'in {self.mode} mode'
        )
        
        # Get Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Handle truncate-insert mode
        if self.mode == "truncate-insert":
            self.log.info(f"Truncating table {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")
        
        # Build and execute INSERT statement
        insert_sql = f"INSERT INTO {self.table} {self.sql}"
        
        self.log.info(
            f"Executing INSERT statement for dimension table {self.table}"
        )
        redshift.run(insert_sql)
        
        self.log.info(
            f'LoadDimensionOperator completed for table {self.table}'
        )
