"""
format_stock_prices 系のテストモジュール

- format_stock_prices(): リスト構造＋型＋キーの検証
- format_stock_prices_by_date(): 日付ごとのグルーピングと構造の検証
"""

import pytest
from utils.etl.format_stock_prices import (
    format_stock_prices,
    format_stock_prices_by_date,
)


@pytest.fixture
def raw_fetch_result():
    """
    yfinance の fetch 関数が返す形式を模したダミーデータ。
    """
    return [
        {
            "ticker": "AAPL",
            "date": "2025-08-02",
            "Open": 192.34,
            "High": 195.00,
            "Low": 190.12,
            "Close": 193.45,
            "Volume": 32000000,
        }
    ]


def test_format_stock_prices_returns_expected_structure(raw_fetch_result):
    """
    format_stock_prices() が想定通りの形式で整形されるかを検証する。
    """
    formatted, stock_date = format_stock_prices(raw_fetch_result)

    # 結果がリスト形式であること
    assert isinstance(formatted, list)
    assert len(formatted) == 1

    # 含まれるキーと型の検証
    result = formatted[0]
    expected_keys = [
        "ticker_id",
        "date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "created_at",
    ]
    for key in expected_keys:
        assert key in result

    assert isinstance(result["ticker_id"], str)
    assert isinstance(result["date"], str)
    assert isinstance(result["open_price"], float)
    assert isinstance(result["high_price"], float)
    assert isinstance(result["low_price"], float)
    assert isinstance(result["close_price"], float)
    assert isinstance(result["volume"], int)
    assert isinstance(result["created_at"], str)


def test_format_stock_prices_by_date_groups_by_date(raw_fetch_result):
    """
    format_stock_prices_by_date() が日付単位で正しくグルーピングされるかを検証する。
    """
    grouped = format_stock_prices_by_date(raw_fetch_result)

    # グルーピングの形式が正しいこと
    assert isinstance(grouped, dict)
    assert "2025-08-02" in grouped
    assert isinstance(grouped["2025-08-02"], list)
    assert len(grouped["2025-08-02"]) == 1

    # 各要素のキー構造を確認
    result = grouped["2025-08-02"][0]
    expected_keys = [
        "ticker_id",
        "date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "created_at",
    ]
    for key in expected_keys:
        assert key in result
