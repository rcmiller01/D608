# BaseOperator + PostgresHook imports that work on AF 1.10.x and 2.x
try:
    from airflow.models import BaseOperator
except Exception:
    from airflow.operators import BaseOperator

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except Exception:
    from airflow.hooks.postgres_hook import PostgresHook


class LoadFactOperator(BaseOperator):
    ui_color = "#F98866"

    def __init__(self, *, redshift_conn_id="redshift", table="", sql="", append_only=False, **kwargs):
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append_only:
            self.log.info("Truncating fact table %s", self.table)
            hook.run(f"TRUNCATE TABLE {self.table};")
        insert_sql = f"INSERT INTO {self.table} {self.sql}"
        self.log.info("Loading fact table %s", self.table)
        hook.run(insert_sql)
        self.log.info("Fact load complete for %s", self.table)
