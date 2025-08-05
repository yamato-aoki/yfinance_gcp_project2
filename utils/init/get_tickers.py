"""
BigQuery上の tickers テーブルから銘柄ID（ticker_id）を取得します。
※現時点では is_index などのフィルタはかけず、全件取得します。
"""

# utils/init/get_tickers.py

# --- サードパーティ ---
from google.cloud import bigquery

# ----------------------------
# BigQueryから銘柄リストを取得
# ----------------------------


def get_tickers(
    dataset: str = "yfinance_analytics", table: str = "tickers"
) -> list[str]:
    """
    BigQueryの tickers テーブルから ticker_id を全件取得する。

    Args:
        dataset (str): 対象データセット（例: yfinance_analytics）
        table (str): 対象テーブル（例: tickers）

    Returns:
        list[str]: ticker_id のリスト（例: ["7203.T", "6758.T", ...]）
    """
    client = bigquery.Client()
    query = f"""
        SELECT ticker_id
        FROM `{dataset}.{table}`
    """
    results = client.query(query).result()
    return [row["ticker_id"] for row in results]
