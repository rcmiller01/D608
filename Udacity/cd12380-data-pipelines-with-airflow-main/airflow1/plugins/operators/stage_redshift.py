from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 copy_json_option="auto",
                 iam_role_arn=None,
                 truncate=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.copy_json_option = copy_json_option
        self.iam_role_arn = iam_role_arn
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        self.log.info("Staging from %s to %s", s3_path, self.table)

        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table};")

        if not self.iam_role_arn:
            raise ValueError("iam_role_arn is required when not using key/secret credentials")

        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            REGION '{self.region}'
            FORMAT AS JSON '{self.copy_json_option}'
            TIMEFORMAT AS 'epochmillisecs'
            iam_role '{self.iam_role_arn}'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
        """
        self.log.info(copy_sql)
        redshift.run(copy_sql)
        self.log.info("Stage complete for %s", self.table)





