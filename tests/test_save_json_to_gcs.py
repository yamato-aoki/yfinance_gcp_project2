"""
save_json_to_gcs() のテストモジュール

- GCS クライアントをモック化して、実際のクラウド操作を伴わずに検証
- モックを通じて storage.Client → bucket → blob → upload の一連の呼び出しが行われたか確認
- アップロードされた内容が JSON 文字列またはバイト列であることを検証
"""

from unittest import mock
from utils.etl.save_json_to_gcs import save_json_to_gcs


@mock.patch("utils.etl.save_json_to_gcs.storage.Client")
def test_save_json_to_gcs_success(mock_storage_client):
    """
    save_json_to_gcs() が storage.Client 経由で upload_from_string() を呼び出すかを検証する。
    """
    # モックの構成：storage.Client → bucket → blob → upload_from_string
    mock_bucket = mock.Mock()
    mock_blob = mock.Mock()
    mock_storage_client.return_value.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    # テスト用ダミーデータ
    test_data = [
        {"ticker_id": "AAPL", "date": "2025-08-03", "open_price": 192.0}
    ]
    bucket_name = "dummy-bucket"

    # 実行
    save_json_to_gcs(bucket_name, test_data)

    # 呼び出し確認
    mock_storage_client.assert_called_once()
    mock_bucket.blob.assert_called_once()
    mock_blob.upload_from_string.assert_called_once()

    # アップロードされた内容の形式を検証
    args, kwargs = mock_blob.upload_from_string.call_args
    assert isinstance(
        args[0], (str, bytes)
    ), "GCSにアップロードされるデータが文字列またはバイト列である必要があります"
