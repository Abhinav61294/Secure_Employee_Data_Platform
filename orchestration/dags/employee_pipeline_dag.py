from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.operators.beam import (
    BeamRunPythonPipelineOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)


PROJECT_ID = "secure-employee-data-platform"
REGION = "us-east1"

BEAM_PIPELINE = (
    "gs://secure-employee-data-platform-data/"
    "beam/employee_pipeline.py"
)

DATAFLOW_TEMP_LOCATION = (
    "gs://secure-employee-data-platform-data/"
    "dataflow/temp"
)

DATAFLOW_STAGING_LOCATION = (
    "gs://secure-employee-data-platform-data/"
    "dataflow/staging"
)

DATAFORM_REPOSITORY = "secure-employee-dataform"
DATAFORM_WORKSPACE = "employee-dataform-dev"


with DAG(
    dag_id="employee_data_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["employee-data", "portfolio"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    run_beam_pipeline = BeamRunPythonPipelineOperator(
        task_id="run_beam_pipeline",
        runner="DataflowRunner",
        py_file=BEAM_PIPELINE,
        pipeline_options={
            "project": PROJECT_ID,
            "region": REGION,
            "temp_location": DATAFLOW_TEMP_LOCATION,
            "staging_location": DATAFLOW_STAGING_LOCATION,
            "job_name": "employee-data-beam-ingestion",
            "zone": "us-east1-b",
        },
    )

    validate_bigquery = GCSToBigQueryOperator(
        task_id="validate_bigquery",
        bucket="secure-employee-data-platform-data",
        source_objects=[
            "processed/masked_employees-*.csv"
        ],
        destination_project_dataset_table=(
            "secure-employee-data-platform."
            "employee_data_staging.beam_employees"
        ),
        schema_fields=[
            {"name": "employee_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
            {"name": "date_of_birth", "type": "DATE", "mode": "NULLABLE"},
            {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
            {"name": "department_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "job_title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location", "type": "STRING", "mode": "NULLABLE"},
            {"name": "hire_date", "type": "DATE", "mode": "NULLABLE"},
            {
                "name": "employment_status",
                "type": "STRING",
                "mode": "NULLABLE",
            },
            {
                "name": "employment_duration_years",
                "type": "INTEGER",
                "mode": "NULLABLE",
            },
        ],
        source_format="CSV",
        skip_leading_rows=0,
        write_disposition="WRITE_TRUNCATE",
    )

    compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_dataform_compilation",
        project_id=PROJECT_ID,
        region="us-central1",
        repository_id=DATAFORM_REPOSITORY,
        compilation_result={
            "git_commitish": "main",
        },
    )

    run_dataform = DataformCreateWorkflowInvocationOperator(
    task_id="run_dataform",
    project_id=PROJECT_ID,
    region="us-central1",
    repository_id=DATAFORM_REPOSITORY,
    workflow_invocation={
        "compilation_result": (
            "{{ ti.xcom_pull("
            "task_ids='create_dataform_compilation'"
            ")['name'] }}"
        ),
        "invocation_config": {
            "service_account": (
                "employee-data-airflow-sa@"
                "secure-employee-data-platform.iam.gserviceaccount.com"
            ),
            "included_targets": [
                {
                    "database": PROJECT_ID,
                    "schema": "employee_data_curated",
                    "name": "employee_mart",
                },
                {
                    "database": PROJECT_ID,
                    "schema": "employee_data_curated",
                    "name": "department_mart",
                },
                {
                    "database": PROJECT_ID,
                    "schema": "employee_data_curated",
                    "name": "salary_mart",
                },
            ],
        },
    },
)

    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete"
    )

    (
        start
        >> run_beam_pipeline
        >> validate_bigquery
        >> compilation_result
        >> run_dataform
        >> pipeline_complete
    )