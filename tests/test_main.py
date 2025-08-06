"""
main.py のテスト（Cloud Functionsエントリポイント）

- リクエストに応じて各ETL処理に分岐されるか
- 異常系ハンドリングの確認
"""

from unittest import mock
from main import etl_dispatcher


@mock.patch("main.get_tickers", return_value=["AAPL", "GOOGL"])
@mock.patch("main.handle_request")
def test_etl_dispatcher_success(mock_handle_request, mock_get_tickers):
    """
    正常系：mode=etl のリクエストを想定し、処理が成功するケース
    """
    mock_request = mock.Mock()
    mock_request.get_json.return_value = {
        "mode": "etl",
        "start_date": None,
        "end_date": None
    }

    response_text, status_code = etl_dispatcher(mock_request)

    assert status_code == 200
    assert "successfully" in response_text
    mock_handle_request.assert_called_once()


@mock.patch("main.get_tickers", return_value=["AAPL"])
@mock.patch("main.handle_request", side_effect=Exception("Something went wrong"))
def test_etl_dispatcher_error(mock_handle_request, mock_get_tickers):
    """
    異常系：handle_request 内部で例外が発生したケースの処理確認
    """
    mock_request = mock.Mock()
    mock_request.get_json.return_value = {
        "mode": "etl",
        "start_date": None,
        "end_date": None
    }

    response_text, status_code = etl_dispatcher(mock_request)

    assert status_code == 500
    assert "failed" in response_text
