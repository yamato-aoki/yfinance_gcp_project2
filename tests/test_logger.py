"""
logger モジュールのテスト

- setup_logger: Loggerインスタンスが正しく初期化されるかを確認
- log_to_gcs: GCSアップロード処理が正しく呼ばれるかをmockで確認
"""

import logging
import pytest
from unittest import mock
from utils.logger import setup_logger, log_to_gcs


def test_setup_logger_returns_logger():
    """
    Loggerインスタンスが返却され、指定レベルが設定されているかを確認
    """
    logger = setup_logger("test_logger")
    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.INFO
    assert not logger.propagate
    assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)


@mock.patch("utils.logger.storage.Client")
def test_log_to_gcs_success(mock_storage_client):
    """
    GCSクライアントのblobアップロードが正しく呼ばれるか確認
    """
    mock_blob = mock.Mock()
    mock_bucket = mock.Mock()
    mock_bucket.blob.return_value = mock_blob
    mock_storage_client.return_value.bucket.return_value = mock_bucket

    test_payload = {"status": "success", "message": "Test log"}
    log_to_gcs(test_payload, "dummy-bucket")

    mock_storage_client.assert_called_once()
    mock_bucket.blob.assert_called_once()
    mock_blob.upload_from_string.assert_called_once()
