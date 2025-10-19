from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []

    def execute(self, context):
        self.log.info('DataQualityOperator starting data quality checks')
        
        if not self.tests:
            self.log.warning("No data quality tests provided")
            return
        
        # Get Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Run each test
        for i, test in enumerate(self.tests):
            sql = test.get('sql')
            expected = test.get('expected')
            cmp = test.get('cmp', 'eq')
            
            self.log.info(f"Running data quality test {i+1}/{len(self.tests)}")
            self.log.info(f"SQL: {sql}")
            
            # Execute the test query
            result = redshift.get_first(sql)[0]
            
            self.log.info(f"Test result: {result}, Expected: {expected}")
            
            # Perform comparison based on cmp parameter
            passed = False
            if cmp == 'eq':
                passed = result == expected
            elif cmp == 'gt':
                passed = result > expected
            elif cmp == 'ge':
                passed = result >= expected
            elif cmp == 'lt':
                passed = result < expected
            elif cmp == 'le':
                passed = result <= expected
            elif cmp == 'ne':
                passed = result != expected
            else:
                raise ValueError(f"Unknown comparison operator: {cmp}")
            
            if passed:
                self.log.info(f"✓ Data quality test {i+1} PASSED")
            else:
                error_msg = (
                    f"✗ Data quality test {i+1} FAILED: "
                    f"Expected {result} {cmp} {expected}"
                )
                self.log.error(error_msg)
                raise ValueError(error_msg)
        
        self.log.info(
            f'DataQualityOperator completed successfully. '
            f'All {len(self.tests)} tests passed.'
        )