# Airflow1 Example DAG Project
# 
# This directory contains example implementations and template files for
# the Airflow data pipeline project. All operators use PostgresHook for
# executing SQL against Redshift and include comprehensive logging with
# self.log.info for key operational steps.
#
# Features:
# - PostgresHook integration for Redshift connectivity
# - Detailed logging throughout all operator executions
# - Template operators for staging, fact/dimension loading, data quality
# - Proper error handling and validation