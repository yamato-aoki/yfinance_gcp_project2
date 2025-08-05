"""リクエストの mode に応じて各ETL処理を呼び分けます。"""

# handlers/request_handler.py

# --- 標準ライブラリ ---
import logging
from typing import Any

# --- 自作モジュール ---
from utils.pipeline import run_extract_pipeline, run_extract_range_pipeline
from utils.init.load_masters import initialize_master_tables

# --- ロガー初期化 ---
logger = logging.getLogger(__name__)


def handle_request(mode: str, **kwargs: Any) -> None:
    """
    リクエストされたモードに応じて処理を分岐します。

    パラメータ:
        mode (str): 実行モード（"etl" / "etl_range" / "init_master"）
        kwargs (dict): 必要に応じた追加パラメータを渡す（tickers, start_date, end_dateなど）

    対応モード:
        - "etl": 最新の株価を取得し、ETLパイプラインを実行
        - "etl_range": 指定された日付範囲で株価を取得し、ETLパイプラインを実行
        - "init_master": マスターデータ（tickers / sectors / currencies）を初期ロード
    """
    logger.info(f"[start] handle_request mode={mode}")

    if mode == "etl":
        return run_extract_pipeline(kwargs.get("tickers", []))

    elif mode == "etl_range":
        return run_extract_range_pipeline(
            kwargs.get("tickers", []),
            kwargs.get("start_date"),
            kwargs.get("end_date"),
        )

    elif mode == "init_master":
        return initialize_master_tables(
            bucket_name=kwargs.get("bucket_name"),
            dataset_name=kwargs.get("dataset_name"),
        )

    else:
        logger.error(f"[error] Invalid mode specified: {mode}")
        raise ValueError(f"Invalid mode specified: {mode}")
