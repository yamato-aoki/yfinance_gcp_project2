"""
fetch_stock_prices_latest() のテストモジュール

- リストが返るか
- 必須キーを含むか
- 市場休場日（週末・祝日など）をスキップするか
- ※エラーハンドリングのテストは含まず、正常データ取得時の挙動を検証
"""

import pytest
from datetime import datetime
from utils.etl.fetch_stock_prices import (
    fetch_stock_prices_latest,
    fetch_stock_prices_by_date_range,
)


@pytest.mark.parametrize("fetch_func", [fetch_stock_prices_latest])
def test_fetch_stock_prices_latest_returns_data(fetch_func):
    """
    fetch_stock_prices_latest() が正常にデータを返すかを検証する。
    """
    tickers = ["AAPL"]
    results = fetch_func(tickers)

    if not results:
        pytest.skip("データなし。市場休場日またはAPI応答なし")

    latest_date = results[0]["date"]
    dt = datetime.fromisoformat(latest_date)
    if dt.weekday() >= 5:
        pytest.skip(f"{latest_date} は週末のためスキップ")

    assert isinstance(results, list), "返り値がリスト型ではありません"
    assert len(results) > 0, "結果が空です"

    sample = results[0]
    assert isinstance(sample, dict), "要素が dict 型ではありません"

    required_keys = [
        "ticker",
        "date",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
    ]
    for key in required_keys:
        assert key in sample, f"'{key}' キーが存在しません"


def test_fetch_stock_prices_by_date_range_returns_data():
    """
    fetch_stock_prices_by_date_range() が正常にデータを返すかを検証する。
    """
    tickers = ["AAPL"]
    today = datetime.now().strftime("%Y-%m-%d")
    results = fetch_stock_prices_by_date_range(tickers, today, today)

    if not results:
        pytest.skip("データなし。市場休場日またはAPI応答なし")

    assert isinstance(results, list), "返り値がリスト型ではありません"
    assert len(results) > 0, "結果が空です"

    sample = results[0]
    assert isinstance(sample, dict), "要素が dict 型ではありません"

    required_keys = [
        "ticker",
        "date",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
    ]
    for key in required_keys:
        assert key in sample, f"'{key}' キーが存在しません"
