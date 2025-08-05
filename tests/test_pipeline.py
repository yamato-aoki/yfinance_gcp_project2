"""
pipeline モジュールのテスト

- run_extract_pipeline()：最新データのETLフローが正しく呼び出されるか
- run_extract_range_pipeline()：日付範囲指定のETLフローが正しく繰り返されるか
- GCS保存／BQロード／マージ／変換／通知までの呼び出し確認（Mockを使用）
"""

from unittest import mock
from utils.pipeline import run_extract_pipeline, run_extract_range_pipeline


@mock.patch("utils.pipeline.notify_slack")
@mock.patch("utils.pipeline.log_to_gcs")
@mock.patch("utils.pipeline.transform_to_analytics_table")
@mock.patch("utils.pipeline.delete_temp_table")
@mock.patch("utils.pipeline.merge_temp_table_to_bq")
@mock.patch("utils.pipeline.load_temp_table")
@mock.patch("utils.pipeline.save_json_to_gcs")
@mock.patch("utils.pipeline.format_stock_prices")
@mock.patch("utils.pipeline.fetch_stock_prices_latest")
def test_run_extract_pipeline(
    mock_fetch,
    mock_format,
    mock_save,
    mock_load,
    mock_merge,
    mock_delete,
    mock_transform,
    mock_log,
    mock_notify,
):
    """
    run_extract_pipeline() が ETL処理の各ステップを 1 回ずつ呼び出すかを検証する。
    """
    test_tickers = ["AAPL", "MSFT"]

    # ダミーデータ設定
    dummy_fetch = [
        {
            "ticker": "AAPL",
            "date": "2025-08-01",
            "Open": 100,
            "High": 110,
            "Low": 90,
            "Close": 105,
            "Volume": 100000,
        }
    ]
    dummy_format = {
        "2025-08-01": [
            {
                "ticker_id": "AAPL",
                "date": "2025-08-01",
                "open_price": 100,
                "high_price": 110,
                "low_price": 90,
                "close_price": 105,
                "volume": 100000,
            }
        ]
    }

    mock_fetch.return_value = dummy_fetch
    mock_format.return_value = (dummy_format, "2025-08-01")

    # 実行
    run_extract_pipeline(test_tickers)

    # 各ステップが 1 回呼ばれていることを検証
    mock_fetch.assert_called_once_with(test_tickers)
    mock_format.assert_called_once_with(dummy_fetch)
    mock_save.assert_called_once()
    mock_load.assert_called_once()
    mock_merge.assert_called_once()
    mock_delete.assert_called_once()
    mock_transform.assert_called_once()
    mock_log.assert_called_once()
    mock_notify.assert_called_once()


@mock.patch("utils.pipeline.notify_slack")
@mock.patch("utils.pipeline.log_to_gcs")
@mock.patch("utils.pipeline.transform_to_analytics_table")
@mock.patch("utils.pipeline.delete_temp_table")
@mock.patch("utils.pipeline.merge_temp_table_to_bq")
@mock.patch("utils.pipeline.load_temp_table")
@mock.patch("utils.pipeline.save_json_to_gcs")
@mock.patch("utils.pipeline.format_stock_prices_by_date")
@mock.patch("utils.pipeline.fetch_stock_prices_by_date_range")
def test_run_extract_range_pipeline(
    mock_fetch,
    mock_format,
    mock_save,
    mock_load,
    mock_merge,
    mock_delete,
    mock_transform,
    mock_log,
    mock_notify,
):
    """
    run_extract_range_pipeline() が 各日付ごとに ETL 処理を繰り返すかを検証する。
    """
    tickers = ["AAPL"]
    start_date = "2025-08-01"
    end_date = "2025-08-02"

    # ダミーの取得データ
    dummy_raw = {
        "2025-08-01": [
            {
                "ticker": "AAPL",
                "date": "2025-08-01",
                "Open": 100,
                "High": 110,
                "Low": 90,
                "Close": 105,
                "Volume": 100000,
            }
        ],
        "2025-08-02": [
            {
                "ticker": "AAPL",
                "date": "2025-08-02",
                "Open": 101,
                "High": 111,
                "Low": 91,
                "Close": 106,
                "Volume": 100001,
            }
        ],
    }

    # フォーマット済みダミーデータ
    dummy_format = {
        "2025-08-01": [
            {
                "ticker_id": "AAPL",
                "date": "2025-08-01",
                "open_price": 100,
                "high_price": 110,
                "low_price": 90,
                "close_price": 105,
                "volume": 100000,
            }
        ],
        "2025-08-02": [
            {
                "ticker_id": "AAPL",
                "date": "2025-08-02",
                "open_price": 101,
                "high_price": 111,
                "low_price": 91,
                "close_price": 106,
                "volume": 100001,
            }
        ],
    }

    mock_fetch.return_value = dummy_raw
    mock_format.return_value = dummy_format

    # 実行
    run_extract_range_pipeline(tickers, start_date, end_date)

    # fetch関数が正しく呼ばれているか
    mock_fetch.assert_called_once_with(tickers, start_date, end_date)

    # 日付単位の処理回数を確認
    assert mock_format.call_count == 1
    assert mock_save.call_count == 2
    assert mock_load.call_count == 2
    assert mock_merge.call_count == 2
    assert mock_delete.call_count == 2

    # 最後の変換・ログ・通知処理は1回ずつ
    mock_transform.assert_called_once()
    mock_log.assert_called_once()
    mock_notify.assert_called_once()
