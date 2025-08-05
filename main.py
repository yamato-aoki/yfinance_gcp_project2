"""
Cloud Functions のエントリポイントとして処理を開始します。
リクエストボディに応じて、modeに基づくETL処理（最新・期間指定・マスタ初期化）を分岐実行します。
"""

# main.py

# --- 標準ライブラリ ---
import logging
import os
import traceback
from typing import Tuple

# --- サードパーティ ---
from flask import Request
from google.cloud import logging as cloud_logging

# --- 自作モジュール ---
from handlers.request_handler import handle_request
from utils.init.get_tickers import get_tickers


# --- ARG: 環境変数の読み込み（ローカル／Cloud両対応） ---
BUCKET_NAME: str = os.environ.get("GCS_BUCKET_NAME", "yfinance-project-bucket")
DATASET_ID: str = os.environ.get("BIGQUERY_DATASET_ID", "yfinance_analytics")

# --- Cloud Loggingの初期化 ---
cloud_logging.Client().setup_logging()
logger = logging.getLogger(__name__)


def etl_dispatcher(request: Request) -> Tuple[str, int]:
    """
    GCP Cloud Functions のエントリポイント関数。

    Parameters:
        request (flask.Request): JSON形式のリクエストボディ（mode, start_date, end_date）

    Returns:
        Tuple[str, int]: 結果メッセージとHTTPステータスコード
    """
    try:
        logger.info("[start] ETL function triggered")

        # --- リクエスト受信・パラメータ抽出 ---
        request_json = request.get_json()
        logger.info(f"[info] Received request payload: {request_json}")

        mode: str = request_json.get("mode", "extract")
        tickers: list[str] = get_tickers()
        start_date: str | None = request_json.get("start_date")
        end_date: str | None = request_json.get("end_date")

        # --- モードに応じたETL処理の実行 ---
        handle_request(
            mode=mode,
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            bucket_name=BUCKET_NAME,
            dataset_name=DATASET_ID,
        )

        logger.info("[success] ETL function completed successfully")
        return ("ETL process completed successfully", 200)

    except Exception as e:
        logger.error(f"[error] ETL process failed: {e}")
        logger.error(traceback.format_exc())
        return (f"ETL process failed: {e}", 500)
