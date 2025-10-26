try:
    from airflow.models import BaseOperator
except Exception:
    from airflow.operators import BaseOperator

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except Exception:
    from airflow.hooks.postgres_hook import PostgresHook


class LoadDimensionOperator(BaseOperator):
    ui_color = "#80BD9E"

    def __init__(self, *, redshift_conn_id="redshift", table="", sql="", mode="truncate-insert", **kwargs):
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.mode = mode  # 'truncate-insert' or 'append'

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.mode == "truncate-insert":
            self.log.info("Truncating dimension table %s", self.table)
            hook.run(f"TRUNCATE TABLE {self.table};")
        insert_sql = f"INSERT INTO {self.table} {self.sql}"
        self.log.info("Loading dimension table %s in %s mode", self.table, self.mode)
        hook.run(insert_sql)
        self.log.info("Dimension load complete for %s", self.table)
