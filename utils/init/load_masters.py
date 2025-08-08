"""
tickers, sectors, currencies などマスタデータを初期ロード。
GCS上のNDJSONファイルをBigQueryに読み込み、各マスタテーブルを再作成する。
"""

# utils/init/load_masters.py

# --- 標準ライブラリ ---
import logging
import os

# --- サードパーティ ---
from google.cloud import bigquery, storage

# --- ロガー設定 ---
logger = logging.getLogger(__name__)

# ----------------------------
# GCS → BigQueryへのロード処理
# ----------------------------


def load_ndjson_to_bigquery(
    bucket_name: str, filename: str, table_id: str, schema: list
) -> None:
    """
    GCS上のNDJSONファイルをBigQueryにロード（WRITE_TRUNCATEで上書き）。

    Args:
        bucket_name (str): GCSバケット名
        filename (str): GCSパス（例: master/tickers.ndjson）
        table_id (str): 完全修飾BQテーブルID（例: project.dataset.table）
        schema (list): BQスキーマ定義（bigquery.SchemaField のリスト）

    Returns:
        None
    """
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    # GCSから一時ファイルにダウンロード
    temp_path = f"/tmp/{os.path.basename(filename)}"
    blob = storage_client.bucket(bucket_name).blob(filename)
    blob.download_to_filename(temp_path)

    # テーブル再作成（スキーマごと上書き）
    bq_client.delete_table(table_id, not_found_ok=True)
    table = bigquery.Table(table_id, schema=schema)
    bq_client.create_table(table)

    # データロード
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )

    with open(temp_path, "rb") as f:
        load_job = bq_client.load_table_from_file(
            f, table_id, job_config=job_config
        )
        load_job.result()

    logger.info(
        f"[success] load_ndjson_to_bigquery: Loaded {filename} to {table_id}"
    )


# ----------------------------
# 全マスタテーブルを一括初期化
# ----------------------------


def initialize_master_tables(bucket_name: str, dataset_name: str) -> None:
    """
    GCS上のマスタファイルをBigQueryに初期ロードする。

    Args:
        bucket_name (str): GCSバケット名（例: yfinance-project-bucket）
        dataset_name (str): BQデータセット名（例: yfinance_analytics）

    Returns:
        None
    """
    project_id = os.environ.get("GCP_PROJECT", "yfinance-project2")

    master_schemas = {
        "tickers": [
            bigquery.SchemaField("ticker_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("company_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sector_id", "STRING"),
            bigquery.SchemaField("currency_id", "STRING"),
        ],
        "sectors": [
            bigquery.SchemaField("sector_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sector_name", "STRING", mode="REQUIRED"),
        ],
        "currencies": [
            bigquery.SchemaField("currency_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("currency_name", "STRING", mode="REQUIRED"),
        ],
    }

    for master_name, schema in master_schemas.items():
        filename = f"master/{master_name}.ndjson"
        table_id = f"{project_id}.{dataset_name}.{master_name}"

        load_ndjson_to_bigquery(
            bucket_name=bucket_name,
            filename=filename,
            table_id=table_id,
            schema=schema,
        )
