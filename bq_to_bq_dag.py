from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(0),
    'retries': 1,
}

with DAG(
    dag_id='bq_to_bq_pipeline',
    default_args=default_args,
    schedule_interval='* * * * *',
    catchup=False,
    tags=['bigquery', 'example'],
) as dag:

    bq_transfer = BigQueryInsertJobOperator(
        task_id='transfer_claim_to_accident',
        configuration={
            "query": {
                "query": """
                    SELECT * FROM `bigquery-public-data.fhir_synthea.claim`
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "ejazgcp",
                    "datasetId": "synthea_ds",
                    "tableId": "accident_data",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED"
            }
        },
        location='US'
    )