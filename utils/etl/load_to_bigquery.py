"""
GCSからBigQueryへロードし、正規化テーブルへのマージ処理を行うモジュール。

- 一時テーブルへのロード（load_temp_table）
- 本テーブルへのMERGE（merge_temp_table_to_bq）
- 一時テーブルの削除（delete_temp_table）
"""

# utils/etl/load_to_bigquery.py

# --- 標準ライブラリ ---
import json
import logging
import uuid

# --- サードパーティ ---
from google.cloud import bigquery, storage

# --- ロガー設定 ---
logger = logging.getLogger(__name__)

# ----------------------------
# 一時テーブルへのロード処理
# ----------------------------


def load_temp_table(
    bucket_name: str,
    json_path: str,
    dataset_id: str,
    table_id: str,
    schema_blob_path: str,
) -> str:
    """
    GCSのndjsonファイルを読み込み、一時テーブルとしてBigQueryにロードする。

    Args:
        bucket_name (str): 対象のGCSバケット名
        json_path (str): GCS内のndjsonファイルパス（例: fact/stock_prices_2025-08-04.ndjson）
        dataset_id (str): BQのデータセットID
        table_id (str): メインテーブルID（例: stock_prices）
        schema_blob_path (str): GCS上に保存されたスキーマ定義ファイル（JSON）

    Returns:
        str: 作成された一時テーブル名（例: stock_prices_temp_a1b2c3d4）
    """
    logger.info(
        "[start] load_temp_table: Loading JSON from GCS into temporary BQ table."
    )

    bq_client = bigquery.Client()
    storage_client = storage.Client()

    # GCSからスキーマJSONを取得 → BigQueryのスキーマ形式に変換
    blob = storage_client.bucket(bucket_name).blob(schema_blob_path)
    schema_json = json.loads(blob.download_as_text())

    schema = []
    for field in schema_json:
        name = field["name"]
        field_type = field["field_type"]
        mode = field.get("mode", "NULLABLE")
        schema.append(bigquery.SchemaField(name, field_type, mode))

    # 一時テーブル名の生成（UUIDでユニークに）
    uri = f"gs://{bucket_name}/{json_path}"
    temp_table_id = f"{table_id}_temp_{uuid.uuid4().hex[:8]}"
    table_ref = f"{bq_client.project}.{dataset_id}.{temp_table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )

    try:
        load_job = bq_client.load_table_from_uri(
            uri, table_ref, job_config=job_config
        )
        load_job.result()
        logger.info(
            f"[success] load_temp_table: Temporary table created: {table_ref}"
        )
        return temp_table_id
    except Exception as e:
        logger.error(
            f"[error] load_temp_table: Failed to create temporary table. Error: {str(e)}"
        )
        raise


# ----------------------------
# MERGE設定（静的変数）
# ----------------------------

DATASET_ID = "yfinance_analytics"
MAIN_TABLE_ID = "stock_prices"
KEY_COLS = ["ticker_id", "date"]
VALUE_COLS = [
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "created_at",
]


# ----------------------------
# MERGE SQLの組み立て
# ----------------------------


def build_merge_sql(
    target_table: str,
    temp_table: str,
    key_cols: list[str],
    value_cols: list[str],
) -> str:
    """
    BigQueryのMERGE文を動的に生成する。

    Args:
        target_table (str): MERGE対象の本テーブル（例: project.dataset.stock_prices）
        temp_table (str): 一時テーブル（例: project.dataset.stock_prices_temp_xxxx）
        key_cols (list[str]): マッチ条件となるキー列（主キー）
        value_cols (list[str]): 更新・挿入対象のカラム

    Returns:
        str: 完成されたMERGE SQL文（マルチライン）
    """
    on_clause = " AND ".join([f"T.{col} = S.{col}" for col in key_cols])
    update_clause = ", ".join([f"{col} = S.{col}" for col in value_cols])
    insert_cols = key_cols + value_cols
    insert_cols_str = ", ".join(insert_cols)
    insert_vals_str = ", ".join([f"S.{col}" for col in insert_cols])

    return f"""
    MERGE `{target_table}` T
    USING `{temp_table}` S
    ON {on_clause}
    WHEN MATCHED THEN
      UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols_str}) VALUES ({insert_vals_str})
    """


# ----------------------------
# MERGE実行
# ----------------------------


def merge_temp_table_to_bq(temp_table_id: str) -> None:
    """
    一時テーブルの内容を、本テーブルに対してMERGE（UPSERT）する。

    Args:
        temp_table_id (str): 一時テーブル名（load_temp_tableの戻り値）
    """
    logger.info(
        f"[start] merge_temp_table_to_bq: Starting merge of temp table {temp_table_id}."
    )
    client = bigquery.Client()
    project = client.project
    target_table = f"{project}.{DATASET_ID}.{MAIN_TABLE_ID}"
    temp_table = f"{project}.{DATASET_ID}.{temp_table_id}"

    merge_sql = build_merge_sql(target_table, temp_table, KEY_COLS, VALUE_COLS)

    try:
        query_job = client.query(merge_sql)
        query_job.result()
        logger.info(
            f"[success] merge_temp_table_to_bq: Merge completed. {temp_table_id} merged into {target_table}"
        )
    except Exception as e:
        logger.error(
            f"[error] merge_temp_table_to_bq: Failed to merge {temp_table_id} into {target_table}. Error: {str(e)}"
        )
        raise


# ----------------------------
# 一時テーブル削除処理
# ----------------------------


def delete_temp_table(temp_table_id: str, dataset_id: str) -> None:
    """
    指定された一時テーブルを削除する。

    Args:
        temp_table_id (str): 一時テーブルID（例: stock_prices_temp_xxxx）
        dataset_id (str): 対象のデータセットID（通常 "yfinance_analytics"）
    """
    logger.info(
        f"[start] delete_temp_table: Deleting temp table {temp_table_id}."
    )
    client = bigquery.Client()
    full_id = f"{client.project}.{dataset_id}.{temp_table_id}"

    try:
        client.delete_table(full_id, not_found_ok=True)
        logger.info(
            f"[success] delete_temp_table: Temporary table deleted: {full_id}"
        )
    except Exception as e:
        logger.error(
            f"[error] delete_temp_table: Failed to delete temporary table {full_id}. Error: {str(e)}"
        )
        raise
