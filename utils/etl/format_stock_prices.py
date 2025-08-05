"""
取得した株価データを整形・加工するモジュール。

- format_stock_prices(): リスト形式で整形（最新データ向け）
- format_stock_prices_by_date(): 日付ごとにグルーピングして整形（日付範囲向け）

整形後のデータは、ETLフローでGCS保存 → BigQueryロードに使用される。
"""

# utils/etl/format_stock_prices.py

# --- 標準ライブラリ ---
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, DefaultDict, Dict, List, Tuple
import logging

# --- 定数 & ロガー ---
JST = timezone(timedelta(hours=9))
logger = logging.getLogger(__name__)


def format_stock_prices(
    fetch_results: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], str]:
    """
    fetch_results をリスト形式で整形し、1日分の株価リストとして返す。

    - return: (整形済みリスト, 対象日付文字列)
    """
    logger.info("[start] format_stock_prices: Starting formatting process.")
    formated_results = []
    stock_date = None

    for fetch_result in fetch_results:
        date = fetch_result["date"][:10]
        if not stock_date:
            stock_date = date

        formatted = {
            "ticker_id": fetch_result["ticker"],
            "date": date,
            "open_price": float(fetch_result["Open"]),
            "high_price": float(fetch_result["High"]),
            "low_price": float(fetch_result["Low"]),
            "close_price": float(fetch_result["Close"]),
            "volume": int(fetch_result["Volume"]),
            "created_at": datetime.now(timezone.utc)
            .astimezone(JST)
            .strftime("%Y-%m-%d %H:%M:%S"),
        }

        formated_results.append(formatted)
        logger.info(
            f"[success] format_stock_prices: [{formatted['ticker_id']}] Formatted data for {formatted['date']}."
        )

    return formated_results, stock_date


def format_stock_prices_by_date(
    fetch_results: List[Dict[str, Any]],
) -> DefaultDict[str, List[Dict[str, Any]]]:
    """
    fetch_results を日付ごとにグルーピングして整形する。

    - return: {"日付文字列": [整形済レコード, ...]} の形式の辞書
    """
    logger.info(
        "[start] format_stock_prices_by_date: Starting daily formatting process."
    )
    grouped = defaultdict(list)

    for fetch_result in fetch_results:
        date = fetch_result["date"][:10]

        formatted = {
            "ticker_id": fetch_result["ticker"],
            "date": date,
            "open_price": float(fetch_result["Open"]),
            "high_price": float(fetch_result["High"]),
            "low_price": float(fetch_result["Low"]),
            "close_price": float(fetch_result["Close"]),
            "volume": int(fetch_result["Volume"]),
            "created_at": datetime.now(timezone.utc)
            .astimezone(JST)
            .strftime("%Y-%m-%d %H:%M:%S"),
        }

        grouped[date].append(formatted)

    for date, records in grouped.items():
        logger.info(
            f"[success] format_stock_prices_by_date: [{date}] Formatted {len(records)} records."
        )

    return grouped
