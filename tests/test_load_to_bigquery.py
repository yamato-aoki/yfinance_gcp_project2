"""
load_to_bigquery モジュールのテスト

- GCS → BigQuery へのロード処理（load_temp_table）
- 一時テーブル → 本テーブルへの MERGE（merge_temp_table_to_bq）
- 一時テーブルの削除処理（delete_temp_table）

いずれも GCP 実行は避け、Mock による呼び出し確認を行う。
"""

from unittest import mock
from utils.etl.load_to_bigquery import (
    load_temp_table,
    merge_temp_table_to_bq,
    delete_temp_table,
)


@mock.patch("utils.etl.load_to_bigquery.bigquery.Client")
@mock.patch("utils.etl.load_to_bigquery.storage.Client")
def test_load_temp_table_calls_load_from_uri(
    mock_storage_client, mock_bq_client
):
    """
    load_temp_table() が load_table_from_uri を正しく呼び出すかを検証する。
    """
    # Storage クライアントのモック
    mock_blob = mock.Mock()
    mock_blob.download_as_text.return_value = (
        '[{"name": "date", "field_type": "DATE"}]'
    )
    mock_bucket = mock.Mock()
    mock_bucket.blob.return_value = mock_blob
    mock_storage = mock.Mock()
    mock_storage.bucket.return_value = mock_bucket
    mock_storage_client.return_value = mock_storage

    # BigQuery クライアントのモック
    mock_bq = mock.Mock()
    mock_bq.load_table_from_uri.return_value.result.return_value = None
    mock_bq_client.return_value = mock_bq

    # 実行
    load_temp_table(
        bucket_name="dummy-bucket",
        json_path="dummy-file.ndjson",
        dataset_id="dummy_dataset",
        table_id="dummy_table",
        schema_blob_path="schema/stock_prices_schema.json",
    )

    # 検証：load_table_from_uri が正しく呼ばれているか
    mock_bq_client.assert_called_once()
    mock_bq.load_table_from_uri.assert_called_once()
    args, kwargs = mock_bq.load_table_from_uri.call_args
    assert "dummy-bucket" in args[0]
    assert "dummy_table" in args[1]


@mock.patch("utils.etl.load_to_bigquery.bigquery.Client")
def test_merge_temp_table_to_bq_executes_merge_sql(mock_bq_client):
    """
    merge_temp_table_to_bq() が MERGE クエリを実行するかを検証する。
    """
    mock_client = mock.Mock()
    mock_client.project = "dummy_project"
    mock_bq_client.return_value = mock_client

    # 実行
    merge_temp_table_to_bq("dummy_temp_table")

    # 検証：MERGE クエリが含まれているか
    mock_bq_client.assert_called_once()
    mock_client.query.assert_called_once()
    assert "MERGE" in mock_client.query.call_args[0][0]


@mock.patch("utils.etl.load_to_bigquery.bigquery.Client")
def test_delete_temp_table_executes_delete(mock_bq_client):
    """
    delete_temp_table() が delete_table を正しく呼び出すかを検証する。
    """
    mock_client = mock.Mock()
    mock_client.project = "dummy_project"
    mock_bq_client.return_value = mock_client

    # 実行
    delete_temp_table(
        temp_table_id="dummy_temp_table", dataset_id="dummy_dataset"
    )

    # 検証：delete_table が適切に呼び出されているか
    mock_bq_client.assert_called_once()
    mock_client.delete_table.assert_called_once()
    assert "dummy_temp_table" in mock_client.delete_table.call_args[0][0]
