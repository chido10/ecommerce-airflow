# dags/ecommerce_pipeline.py
#
# Daily ecommerce orchestration with identifiers from Airflow Variables:
#   - glue_crawler_name, glue_job_name
#   - ecom_s3_bucket, ecom_s3_base
#   - ecom_redshift_conf (JSON: cluster_id, database, db_user)
#   - sns_topic_arn (optional; if empty, just logs)
#
# Schedule: 19:40 UTC daily

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ---- variables (all safe defaults are placeholders) ----
GLUE_CRAWLER_NAME = Variable.get("glue_crawler_name", default_var="ecommerce-raw-crawler")
GLUE_JOB_NAME     = Variable.get("glue_job_name",     default_var="ecommerce-etl-job")

S3_BUCKET = Variable.get("ecom_s3_bucket", default_var="your-bucket-here")
S3_BASE   = Variable.get("ecom_s3_base",   default_var="raw")  # raw/<domain>/<YYYY-MM-DD>/

REDSHIFT_CONF_VAR = "ecom_redshift_conf"  # JSON Variable
REDSHIFT_TABLES = {
    "dim_customers":   {"min_count": 0},
    "dim_products":    {"min_count": 1},
    "fact_orders":     {"min_count": 1},
    "agg_daily_sales": {"min_count": 0},
}

CRAWLER_POKE_SECONDS = 30
JOB_POKE_SECONDS     = 30
MAX_WAIT_MINUTES     = 90

def _aws_client(service_name: str):
    return AwsBaseHook(aws_conn_id="aws_default", client_type=service_name).get_conn()

def _today_yyyy_mm_dd() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def s3_presence_check(domains: list[str] = None, **_):
    if domains is None:
        domains = ["customers", "orders", "products"]

    s3 = S3Hook(aws_conn_id="aws_default")
    today = _today_yyyy_mm_dd()
    missing = []

    for d in domains:
        prefix = f"{S3_BASE}/{d}/{today}/"
        keys = s3.list_keys(bucket_name=S3_BUCKET, prefix=prefix) or []
        logging.info("S3 check for %s -> prefix '%s' (first keys: %s)", d, prefix, keys[:5] if keys else "None")
        if not keys:
            missing.append(f"{d} ({prefix})")

    if missing:
        raise RuntimeError("S3 'today' partition missing content for: " + ", ".join(missing))
    logging.info("S3 presence OK for %s on %s", ", ".join(domains), today)

def start_glue_crawler(**_):
    glue = _aws_client("glue")
    logging.info("Starting Glue crawler: %s", GLUE_CRAWLER_NAME)
    glue.start_crawler(Name=GLUE_CRAWLER_NAME)

def wait_for_crawler_ready(**_):
    glue = _aws_client("glue")
    deadline = time.time() + MAX_WAIT_MINUTES * 60
    while True:
        crawler = glue.get_crawler(Name=GLUE_CRAWLER_NAME)["Crawler"]
        state = crawler.get("State")
        logging.info("Crawler state=%s", state)
        if state != "RUNNING":
            break
        if time.time() > deadline:
            raise TimeoutError("Timed out waiting for crawler to finish RUNNING.")
        time.sleep(CRAWLER_POKE_SECONDS)

    last_crawl = glue.get_crawler(Name=GLUE_CRAWLER_NAME)["Crawler"].get("LastCrawl") or {}
    status = last_crawl.get("Status")  # SUCCEEDED | FAILED | CANCELLED | None
    logging.info("Crawler LastCrawl: %s", json.dumps(last_crawl, default=str))
    if status and status != "SUCCEEDED":
        raise RuntimeError(f"Glue crawler '{GLUE_CRAWLER_NAME}' last status={status}")
    logging.info("Crawler %s finished successfully.", GLUE_CRAWLER_NAME)

def start_glue_job(**_):
    glue = _aws_client("glue")
    logging.info("Starting Glue job: %s", GLUE_JOB_NAME)
    job_run_id = glue.start_job_run(JobName=GLUE_JOB_NAME)["JobRunId"]
    logging.info("Glue job run id: %s", job_run_id)
    return job_run_id

def wait_for_glue_job(**context):
    glue = _aws_client("glue")
    job_run_id = context["ti"].xcom_pull(task_ids="start_glue_job")
    if not job_run_id:
        raise RuntimeError("No JobRunId found in XCom from start_glue_job")

    logging.info("Waiting for Glue job %s run %s", GLUE_JOB_NAME, job_run_id)
    deadline = time.time() + MAX_WAIT_MINUTES * 60
    terminal = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"}

    while True:
        state = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_run_id)["JobRun"]["JobRunState"]
        logging.info("Glue job state: %s", state)
        if state in terminal:
            if state != "SUCCEEDED":
                raise RuntimeError(f"Glue job ended in {state}")
            logging.info("Glue job SUCCEEDED.")
            break
        if time.time() > deadline:
            raise TimeoutError("Timed out waiting for Glue job to finish.")
        time.sleep(JOB_POKE_SECONDS)

def _redshift_data_client():
    return _aws_client("redshift-data")

def redshift_checks(**_):
    conf = Variable.get(REDSHIFT_CONF_VAR, deserialize_json=True)
    cluster_id = conf["cluster_id"]
    database   = conf["database"]
    db_user    = conf["db_user"]

    rsd = _redshift_data_client()
    errors, results = [], {}

    for table, rule in REDSHIFT_TABLES.items():
        sql = f"SELECT COUNT(*) AS c FROM {table};"
        logging.info("Executing on Redshift: %s", sql)
        statement_id = rsd.execute_statement(
            ClusterIdentifier=cluster_id, Database=database, DbUser=db_user, Sql=sql
        )["Id"]

        deadline = time.time() + MAX_WAIT_MINUTES * 60
        while True:
            d = rsd.describe_statement(Id=statement_id)
            status = d["Status"]
            if status in ("FINISHED", "FAILED", "ABORTED"):
                break
            if time.time() > deadline:
                raise TimeoutError(f"Timeout waiting for Redshift statement {statement_id}")
            time.sleep(3)

        if d["Status"] != "FINISHED":
            raise RuntimeError(f"Redshift statement failed: {d}")

        rows = rsd.get_statement_result(Id=statement_id).get("Records", [])
        count_val = int(rows[0][0].get("longValue", 0)) if rows else 0
        results[table] = count_val

        if count_val < int(rule.get("min_count", 0)):
            errors.append(f"Table {table}: count {count_val} < required {rule['min_count']}")

    logging.info("Row counts: %s", results)
    if errors:
        raise RuntimeError("Redshift data checks failed: " + "; ".join(errors))
    return results

def _notify(subject: str, payload: dict):
    topic_arn = Variable.get("sns_topic_arn", default_var=None)
    if not topic_arn:
        logging.info("[Notify] %s :: %s", subject, json.dumps(payload))
        return
    sns = _aws_client("sns")
    sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=json.dumps(payload, default=str))
    logging.info("[Notify->SNS:%s] %s", topic_arn, subject)

def notify_success(**context):
    msg = {
        "event": "ecommerce_pipeline_success",
        "run_id": context["run_id"],
        "execution_date": str(context["execution_date"]),
        "redshift_counts": context["ti"].xcom_pull(task_ids="redshift_checks"),
    }
    _notify("SUCCESS: pipeline completed", msg)

def notify_failure(context):
    msg = {
        "event": "ecommerce_pipeline_failure",
        "run_id": context.get("run_id"),
        "execution_date": str(context.get("execution_date")),
        "task": context.get("task_instance").task_id if context.get("task_instance") else None,
        "exception": str(context.get("exception")),
    }
    _notify("FAILURE: pipeline failed", msg)

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="ecommerce_pipeline",
    description="Daily: S3 presence -> Glue crawler -> Glue job -> Redshift checks -> notify",
    default_args=default_args,
    start_date=datetime(2025, 10, 1, tzinfo=timezone.utc),
    schedule="40 19 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "glue", "redshift", "s3"],
) as dag:

    t0_s3_check = PythonOperator(task_id="s3_presence_check", python_callable=s3_presence_check)
    t1_start_crawler = PythonOperator(task_id="start_glue_crawler", python_callable=start_glue_crawler)
    t2_wait_crawler = PythonOperator(task_id="wait_for_crawler_ready", python_callable=wait_for_crawler_ready)
    t3_start_job = PythonOperator(task_id="start_glue_job", python_callable=start_glue_job)
    t4_wait_job = PythonOperator(task_id="wait_for_glue_job", python_callable=wait_for_glue_job)
    t5_checks = PythonOperator(task_id="redshift_checks", python_callable=redshift_checks)
    t6_notify_ok = PythonOperator(task_id="notify_success", trigger_rule=TriggerRule.ALL_SUCCESS, python_callable=notify_success)

    t0_s3_check >> t1_start_crawler >> t2_wait_crawler >> t3_start_job >> t4_wait_job >> t5_checks >> t6_notify_ok
