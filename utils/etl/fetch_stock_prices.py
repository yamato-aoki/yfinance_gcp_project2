"""
yfinance から株価データを取得するモジュール。

- 最新日（前日）の株価を取得（fetch_stock_prices_latest）
- 任意の日付範囲での株価を取得（fetch_stock_prices_by_date_range）

取得データは dict のリスト形式で返され、ETLパイプラインで使用される。
"""

# utils/etl/fetch_stock_prices.py

# --- 標準ライブラリ ---
from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Dict, List

# --- サードパーティ ---
import yfinance as yf

# --- ロガー設定 ---
logger = logging.getLogger(__name__)

# ----------------------------
# 補助関数
# ----------------------------


def get_yesterday_date_str() -> str:
    """
    昨日の日付を UTC 基準で文字列（YYYY-MM-DD）として返す。
    ※ Cloud Scheduler による深夜自動実行を前提とした取得仕様。
    """
    return (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")


# ----------------------------
# メイン取得関数（内部用）
# ----------------------------


def fetch_stock_prices(
    tickers: List[str], start: str, end: str
) -> List[Dict[str, Any]]:
    """
    指定した日付範囲における株価データを yfinance から取得し、整形して返す。

    - start: 取得開始日（YYYY-MM-DD）
    - end:   取得終了日（YYYY-MM-DD, 当日含む）
    - return: dict型のレコードを格納したリスト（"ticker", "date", "Open" 等を含む）
    """
    end_dt = datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)
    end_for_fetch = end_dt.strftime("%Y-%m-%d")
    end_for_display = (end_dt - timedelta(days=1)).strftime("%Y-%m-%d")

    date_label = (
        f"{start}"
        if start == end_for_display
        else f"{start} ~ {end_for_display}"
    )
    fetch_results = []

    for ticker in tickers:
        try:
            logger.info(
                f"[start] fetch_stock_prices: [{ticker}] Fetching stock data for {date_label}..."
            )

            t = yf.Ticker(ticker)
            df = t.history(start=start, end=end_for_fetch)

            # データが取得できなかった場合はスキップ
            if df.empty:
                logger.info(
                    f"[info] fetch_stock_prices: [{ticker}] No data available for {date_label}."
                )
                continue

            # レコード整形
            records = df.reset_index().to_dict(orient="records")
            now = datetime.now(timezone.utc).isoformat()

            for r in records:
                r["ticker"] = ticker
                r["date"] = r["Date"].isoformat()
                r["created_at"] = now
                del r["Date"]

            fetch_results.extend(records)
            logger.info(
                f"[success] fetch_stock_prices: [{ticker}] Successfully fetched data for {date_label}."
            )

        except Exception as e:
            logger.error(
                f"[error] fetch_stock_prices: [{ticker}] Failed to fetch stock data for {date_label}. Error: {str(e)}"
            )

    return fetch_results


# ----------------------------
# 外部公開用関数（ETLの呼び出し側で利用）
# ----------------------------


def fetch_stock_prices_latest(tickers: List[str]) -> List[Dict[str, Any]]:
    """
    最新（前日）の株価データを取得する関数。
    run_extract_pipeline() から呼び出される。
    """
    yesterday = get_yesterday_date_str()
    return fetch_stock_prices(tickers, start=yesterday, end=yesterday)


def fetch_stock_prices_by_date_range(
    tickers: List[str], start_date: str, end_date: str
) -> List[Dict[str, Any]]:
    """
    任意の範囲で株価データを取得する関数。
    run_extract_range_pipeline() から呼び出される。

    - start_date: 取得開始日（YYYY-MM-DD）
    - end_date:   取得終了日（YYYY-MM-DD）
    """
    return fetch_stock_prices(tickers, start=start_date, end=end_date)
