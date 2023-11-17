"""
Example Airflow DAG for testing interaction between Google Cloud Storage and local file system.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import models
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "testing-environment"
PROJECT_ID = os.environ.get("GCP_PROJECT", "TODO: replace with you project_id")

DAG_ID = "gcs_upload_download"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "example_upload.txt"
PATH_TO_SAVED_FILE = "example_upload_download.txt"
UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)


with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["gcs", "example"],
) as dag:
    # [START howto_operator_gcs_create_bucket]
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
    )
    # [END howto_operator_gcs_create_bucket]

    # [START howto_operator_local_filesystem_to_gcs]
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=UPLOAD_FILE_PATH,
        dst=FILE_NAME,
        bucket=BUCKET_NAME,
    )
    # [END howto_operator_local_filesystem_to_gcs]

    # [START howto_operator_gcs_download_file_task]
    download_file = GCSToLocalFilesystemOperator(
        task_id="download_file",
        object_name=FILE_NAME,
        bucket=BUCKET_NAME,
        filename=PATH_TO_SAVED_FILE,
    )
    # [END howto_operator_gcs_download_file_task]

    # [START howto_operator_gcs_delete_bucket]
    delete_bucket = GCSDeleteBucketOperator(task_id="delete_bucket", bucket_name=BUCKET_NAME)
    # [END howto_operator_gcs_delete_bucket]
    delete_bucket.trigger_rule = TriggerRule.ALL_DONE

    (
        # TEST SETUP
        create_bucket
        >> upload_file
        # TEST BODY
        >> download_file
        # TEST TEARDOWN
        >> delete_bucket
    )

