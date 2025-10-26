from typing import Optional

# BaseOperator import that works on Airflow 1.10.x and 2.x
try:
    from airflow.models import BaseOperator  # preferred
except Exception:  # ultra-old fallback
    from airflow.operators import BaseOperator

# PostgresHook import path differs between AF 1.10.x and 2.x
try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # AF 2.x
except Exception:
    from airflow.hooks.postgres_hook import PostgresHook  # AF 1.10.x


class StageToRedshiftOperator(BaseOperator):
    """
    COPY from S3 to Redshift (Serverless-friendly).

    - Uses IAM Role (required) instead of key/secret.
    - Templated s3_key supports date-partitioned prefixes (e.g., {{ execution_date.year }}).
    - Accepts copy_json_option='auto' or a JSONPaths file like 's3://.../log_json_path.json'.
    """

    template_fields = ("s3_bucket", "s3_key", "copy_json_option", "table", "region")
    ui_color = "#358140"

    def __init__(
        self,
        *,
        redshift_conn_id: str = "redshift",
        table: str = "",
        s3_bucket: str = "",
        s3_key: str = "",
        region: str = "us-east-1",
        copy_json_option: Optional[str] = None,   # 'auto' or 's3://.../log_json_path.json'
        json_path: Optional[str] = None,          # legacy alias; if set, overrides copy_json_option
        iam_role_arn: Optional[str] = None,       # REQUIRED
        truncate: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id or "redshift"
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.copy_json_option = json_path if json_path else (copy_json_option or "auto")
        self.iam_role_arn = iam_role_arn
        self.truncate = truncate

    def execute(self, context):
        if not self.iam_role_arn:
            raise ValueError("iam_role_arn is required when not using key/secret credentials")

        # Build S3 URI (avoid accidental double slashes)
        key = (self.s3_key or "").lstrip("/")
        s3_uri = f"s3://{self.s3_bucket}/{key}" if key else f"s3://{self.s3_bucket}"

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info("Truncating table %s", self.table)
            redshift.run(f"TRUNCATE TABLE {self.table};")

        self.log.info(
            "COPY into %s from %s (region=%s, json=%s, role=%s)",
            self.table, s3_uri, self.region, self.copy_json_option, self.iam_role_arn
        )

        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_uri}'
            REGION '{self.region}'
            FORMAT AS JSON '{self.copy_json_option}'
            TIMEFORMAT AS 'epochmillisecs'
            IAM_ROLE '{self.iam_role_arn}'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
        """

        self.log.debug("COPY statement:\n%s", copy_sql.strip())
        redshift.run(copy_sql)
        self.log.info("COPY completed for table %s", self.table)
