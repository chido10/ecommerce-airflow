# dags/smoke_s3.py
#
# Minimal S3 connectivity check for Airflow → AWS integration.
# All identifiers are loaded from Airflow Variables:
#   - smoke_s3_bucket  (your target bucket)
#   - smoke_s3_prefix  (sub-folder path, e.g. "raw/orders/")
#   - smoke_sample_size (optional; how many keys to print, default 5)

from __future__ import annotations

import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

log = logging.getLogger(__name__)

# ---- variable-driven config ----
BUCKET = Variable.get("smoke_s3_bucket", default_var="your-bucket-here")
PREFIX = Variable.get("smoke_s3_prefix", default_var="raw/orders/")
SAMPLE_SIZE = int(Variable.get("smoke_sample_size", default_var="5"))

def s3_check(**_):
    """List a few keys from the configured S3 prefix to confirm connectivity."""
    hook = S3Hook(aws_conn_id="aws_default")
    keys = hook.list_keys(bucket_name=BUCKET, prefix=PREFIX) or []
    sample = keys[:SAMPLE_SIZE]
    log.info("S3 bucket='%s' prefix='%s' -> first %d keys: %s",
             BUCKET, PREFIX, SAMPLE_SIZE, sample if sample else "None")
    log.info("✓ S3 connection verified successfully!")

with DAG(
    dag_id="smoke_s3",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["smoke", "infra"],
) as dag:
    PythonOperator(
        task_id="s3_check",
        python_callable=s3_check,
    )
