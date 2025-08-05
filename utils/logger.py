"""
Cloud Logging（Cloud Functions 標準出力）と、
Cloud Storage（GCS）へのJSONログ保存の両方に対応したロガーモジュール。

- setup_logger(): Cloud Functions/Logging用ロガーの初期化
- log_to_gcs(): GCSバケットへJSON形式でログを保存

Cloud Logging によるモニタリングと、
GCS保存によるエラー/成功履歴の永続化を両立。
"""

# utils/logger.py

# --- 標準ライブラリ ---
import json
import logging
import sys
from datetime import datetime

# --- サードパーティ ---
from google.cloud import logging as cloud_logging
from google.cloud import storage


def setup_logger(name=__name__) -> logging.Logger:
    """
    Cloud Logging に対応した Logger オブジェクトを返す。

    Cloud Functions 上で stdout に出力しつつ、
    Cloud Logging にも送信されるように設定する。

    Args:
        name (str): モジュール名などを指定してロガー名を設定する（__name__ 推奨）

    Returns:
        logging.Logger: 初期化済みのロガーインスタンス
    """
    cloud_client = cloud_logging.Client()
    cloud_client.setup_logging(log_level=logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "[%(asctime)s] %(name)s | %(levelname)s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    # stdout 出力用（Cloud Functions で確認可能）
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


# 共通ロガー（モジュール名ベースで初期化）
logger = setup_logger(__name__)


def log_to_gcs(payload: dict, bucket_name: str) -> None:
    """
    JSONログを GCS（Cloud Storage）に保存する。

    保存先は `logs/YYYY-MM-DD_HHMMSS_status.json` の形式で命名される。
    例: logs/2025-08-06_101530_success.json

    Args:
        payload (dict): ログ内容。statusキー（success, errorなど）によってファイル名が分岐
        bucket_name (str): 保存先の GCS バケット名

    Returns:
        None
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
    status = payload.get("status", "log")
    filename = f"logs/{timestamp}_{status}.json"

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(filename)

        blob.upload_from_string(
            data=json.dumps(payload, ensure_ascii=False, indent=2),
            content_type="application/json",
        )
        logger.info(f"Successfully saved log to GCS: {filename}")
    except Exception as e:
        logger.error(f"Failed to save log to GCS: {e}")
