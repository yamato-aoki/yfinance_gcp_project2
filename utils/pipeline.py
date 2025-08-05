"""
株価ETLパイプラインの統括モジュール。

- run_extract_pipeline(): 最新日の株価データを取得して処理
- run_extract_range_pipeline(): 日付範囲指定で複数日分を一括処理
- handle_etl_success(), handle_etl_error(): 成功／失敗時の通知とログ処理
- handle_etl_skip(): 休日などでデータが空だった場合のスキップ処理

Slack通知・GCSログ保存・Cloud Logging 出力の三段構えで可視化を実現。
"""

# utils/pipeline.py

# --- 標準ライブラリ ---
import logging
import traceback
from datetime import datetime
from typing import List

# --- 自作モジュール ---
from utils.etl.fetch_stock_prices import (
    fetch_stock_prices_latest,
    fetch_stock_prices_by_date_range,
)
from utils.etl.format_stock_prices import (
    format_stock_prices,
    format_stock_prices_by_date,
)
from utils.etl.save_json_to_gcs import save_json_to_gcs
from utils.etl.load_to_bigquery import (
    load_temp_table,
    merge_temp_table_to_bq,
    delete_temp_table,
)
from utils.etl.transform_to_analytics import transform_to_analytics_table
from utils.notify.notifier import notify_slack, format_slack_message
from utils.logger import log_to_gcs

# --- ロガー設定 ---
logger = logging.getLogger(__name__)

# --- 定数 ---
BUCKET_NAME = "yfinance-project-bucket"
DATASET_ID = "yfinance_analytics"

# ----------------------------
# 最新日処理（単日ETL）
# ----------------------------


def run_extract_pipeline(tickers: List[str]) -> None:
    """
    最新株価のETLパイプラインを一括実行する。

    - yfinanceから最新データ取得
    - 整形 → GCS保存 → BQロード → マージ → 非正規化変換
    - Slack通知 / GCSログ出力

    Args:
        tickers (List[str]): 対象のティッカーリスト
    """
    timestamp = datetime.utcnow().isoformat()
    mode = "etl"
    try:
        logger.info(f"[start] run_extract_pipeline: tickers={tickers}")
        fetch_results = fetch_stock_prices_latest(tickers)

        if not fetch_results:
            handle_etl_skip(mode, timestamp)
            return

        formatted_results, stock_date = format_stock_prices(fetch_results)
        save_json_to_gcs(BUCKET_NAME, formatted_results)

        temp_table_id = load_temp_table(
            bucket_name=BUCKET_NAME,
            json_path=f"fact/stock_prices_{stock_date}.ndjson",
            dataset_id=DATASET_ID,
            table_id="stock_prices",
            schema_blob_path="schema/stock_prices_schema.json",
        )
        merge_temp_table_to_bq(temp_table_id)
        delete_temp_table(temp_table_id, DATASET_ID)

        transform_to_analytics_table()

        handle_etl_success(
            mode, timestamp, f"ETL処理（mode: {mode}）が正常に完了しました。"
        )
    except Exception as e:
        handle_etl_error(mode, timestamp, e)


# ----------------------------
# 範囲指定処理（複数日ETL）
# ----------------------------


def run_extract_range_pipeline(
    tickers: List[str], start_date: str, end_date: str
) -> None:
    """
    日付範囲指定で株価のETLパイプラインを実行する。

    - 複数日分の取得・整形・GCS保存・BQロード・マージ
    - 分析テーブル作成
    - Slack通知 / GCSログ出力

    Args:
        tickers (List[str]): 対象ティッカー
        start_date (str): 開始日（YYYY-MM-DD）
        end_date (str): 終了日（YYYY-MM-DD）
    """
    timestamp = datetime.utcnow().isoformat()
    mode = "etl_range"
    try:
        fetch_results = fetch_stock_prices_by_date_range(
            tickers, start_date, end_date
        )

        if not fetch_results:
            handle_etl_skip(mode, timestamp)
            return

        grouped_results = format_stock_prices_by_date(fetch_results)

        for stock_date, daily_results in grouped_results.items():
            save_json_to_gcs(BUCKET_NAME, daily_results)

            temp_table_id = load_temp_table(
                bucket_name=BUCKET_NAME,
                json_path=f"fact/stock_prices_{stock_date}.ndjson",
                dataset_id=DATASET_ID,
                table_id="stock_prices",
                schema_blob_path="schema/stock_prices_schema.json",
            )
            merge_temp_table_to_bq(temp_table_id)
            delete_temp_table(temp_table_id, DATASET_ID)

        transform_to_analytics_table()

        handle_etl_success(
            mode,
            timestamp,
            f"{len(grouped_results)}日分のETL処理が正常に完了しました。",
        )
    except Exception as e:
        handle_etl_error(mode, timestamp, e)


# ----------------------------
# 成功・失敗・スキップハンドラ
# ----------------------------


def handle_etl_skip(
    mode: str,
    timestamp: str,
    reason: str = "No stock data fetched (possibly holiday). ETL skipped.",
) -> None:
    """
    データ取得が空だった場合のスキップ処理。

    Args:
        mode (str): 処理モード
        timestamp (str): 実行時刻（ISO形式）
        reason (str): スキップ理由（デフォルト: 休日など）
    """
    payload = {
        "status": "skip",
        "mode": mode,
        "timestamp": timestamp,
        "message": reason,
    }
    log_to_gcs(payload, BUCKET_NAME)
    notify_slack(format_slack_message(payload), success=True)
    logger.warning(f"[skip] {reason}")


def handle_etl_success(mode: str, timestamp: str, message: str) -> None:
    """
    成功時のSlack通知＋GCSログ保存処理。

    Args:
        mode (str): 処理モード
        timestamp (str): 実行時刻
        message (str): 成功メッセージ
    """
    payload = {
        "status": "success",
        "mode": mode,
        "timestamp": timestamp,
        "message": message,
    }
    log_to_gcs(payload, BUCKET_NAME)
    notify_slack(format_slack_message(payload), success=True)


def handle_etl_error(mode: str, timestamp: str, error: Exception) -> None:
    """
    失敗時のSlack通知＋GCSログ保存処理。

    Args:
        mode (str): 処理モード
        timestamp (str): 実行時刻
        error (Exception): 発生した例外
    """
    error_payload = {
        "status": "error",
        "mode": mode,
        "timestamp": timestamp,
        "error_message": str(error),
        "traceback": traceback.format_exc(),
    }
    log_to_gcs(error_payload, BUCKET_NAME)
    notify_slack(format_slack_message(error_payload), success=False)
    raise error
