try:
    from airflow.models import BaseOperator
except Exception:
    from airflow.operators import BaseOperator

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except Exception:
    from airflow.hooks.postgres_hook import PostgresHook


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    def __init__(self, *, redshift_conn_id="redshift", tests=None, **kwargs):
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for i, t in enumerate(self.tests, 1):
            sql = t["sql"]
            expected = t["expected"]
            cmp = t.get("cmp", "eq")
            self.log.info("DQ Test %d: %s", i, sql)
            res = hook.get_first(sql)
            if res is None:
                raise ValueError(f"Data quality query returned no results: {sql}")
            val = res[0]
            ops = {
                "eq":  lambda a,b: a == b,
                "ne":  lambda a,b: a != b,
                "gt":  lambda a,b: a >  b,
                "ge":  lambda a,b: a >= b,
                "lt":  lambda a,b: a <  b,
                "le":  lambda a,b: a <= b
            }
            ok = ops.get(cmp, ops["eq"])(val, expected)
            if not ok:
                raise AssertionError(f"DQ failed (#{i}): got {val} cmp {cmp} expected {expected} :: {sql}")
        self.log.info("All data quality checks passed.")
