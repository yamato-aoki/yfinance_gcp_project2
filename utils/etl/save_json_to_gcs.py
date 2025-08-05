"""
整形済み株価データを NDJSON 形式で GCS に保存するモジュール。

- 単日データの保存（save_json_to_gcs）
- 複数日（dateごと）の保存（save_json_to_gcs_by_date）
- 保存済ファイルの存在確認（file_exists）
"""

# utils/etl/save_json_to_gcs.py

# --- 標準ライブラリ ---
import json
import logging

# --- サードパーティ ---
from google.cloud import storage

# --- ロガー設定 ---
logger = logging.getLogger(__name__)


def save_json_to_gcs(bucket_name: str, formatted_results: list[dict]) -> None:
    """
    整形済み株価データ（1日分）を NDJSON 形式で GCS に保存する。

    ファイル名形式:
        fact/stock_prices_YYYY-MM-DD.ndjson

    Args:
        bucket_name (str): GCSバケット名
        formatted_results (list[dict]): 整形済みの株価データ（1日分）

    Returns:
        None
    """
    if not formatted_results:
        logger.info(
            "[info] save_json_to_gcs: No data to save. Skipping upload to GCS."
        )
        return

    logger.info(
        f"[start] save_json_to_gcs: Start uploading {len(formatted_results)} records to GCS."
    )

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # ファイル名を日付から決定
    stock_date = formatted_results[0]["date"]
    filename = f"fact/stock_prices_{stock_date}.ndjson"

    # NDJSON形式へ変換
    ndjson_lines = "\n".join(
        json.dumps(r, ensure_ascii=False) for r in formatted_results
    )
    data_bytes = ndjson_lines.encode("utf-8")

    try:
        blob = bucket.blob(filename)
        blob.upload_from_string(
            data_bytes, content_type="application/x-ndjson"
        )
        logger.info(
            f"[success] save_json_to_gcs: File saved to gs://{bucket_name}/{filename}"
        )
    except Exception as e:
        logger.error(
            f"[error] save_json_to_gcs: Failed to upload file to GCS. Error: {str(e)}"
        )
        raise


# ----------------------------
# 複数日分を GCS に保存（日付単位）
# ----------------------------


def save_json_to_gcs_by_date(
    bucket_name: str, grouped_results: dict[str, list[dict]]
) -> None:
    """
    日付ごとにグループ化された株価データを NDJSON 形式で GCS に保存する。

    Args:
        bucket_name (str): GCSバケット名
        grouped_results (dict): {YYYY-MM-DD: [株価レコード]} の形式

    Returns:
        None
    """
    logger.info(
        f"[start] save_json_to_gcs_by_date: Saving grouped results for {len(grouped_results)} dates."
    )

    for date_str, formatted_list in grouped_results.items():
        filename = f"fact/stock_prices_{date_str}.ndjson"
        ndjson_lines = "\n".join(
            json.dumps(r, ensure_ascii=False) for r in formatted_list
        )
        data_bytes = ndjson_lines.encode("utf-8")

        try:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(filename)
            blob.upload_from_string(
                data_bytes, content_type="application/x-ndjson"
            )
            logger.info(
                f"[success] save_json_to_gcs_by_date: File saved to gs://{bucket_name}/{filename}"
            )
        except Exception as e:
            logger.error(
                f"[error] save_json_to_gcs_by_date: Failed to save {filename}. Error: {str(e)}"
            )
            raise


# ----------------------------
# 保存済ファイルの存在確認（補助関数）
# ----------------------------


def file_exists(bucket_name: str, path: str) -> bool:
    """
    GCS 上に指定ファイルが存在するかを確認する。

    Args:
        bucket_name (str): GCSバケット名
        path (str): GCS 上のファイルパス（例: fact/stock_prices_2025-08-04.ndjson）

    Returns:
        bool: ファイルが存在する場合は True、存在しない場合は False
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    return bucket.blob(path).exists()
