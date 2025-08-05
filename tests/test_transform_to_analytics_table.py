"""
transform_to_analytics_table() のテストモジュール

- 非正規化テーブル（stock_prices_analysis）を作成するSQLが正しく実行されるかを確認
- BigQueryクライアントをMock化し、実際のクエリ実行は発生しない形で検証
- 実行されたSQLに期待される構文（CREATE TABLE, SELECT, JOIN, MA列など）が含まれているかをチェック
"""

from unittest import mock
from utils.etl.transform_to_analytics import transform_to_analytics_table


@mock.patch("utils.etl.transform_to_analytics.bigquery.Client")
def test_transform_to_analytics_table_executes_query(mock_bq_client):
    """
    transform_to_analytics_table() が CREATE TABLE クエリを正しく実行するかを検証する。
    """
    # BigQuery クライアントのモックを設定
    mock_client_instance = mock.Mock()
    mock_bq_client.return_value = mock_client_instance

    # 実行
    transform_to_analytics_table()

    # クエリ実行が 1 回だけ呼ばれていることを検証
    mock_bq_client.assert_called_once()
    mock_client_instance.query.assert_called_once()

    # 実行された SQL に重要な構文が含まれているか確認
    query_arg = mock_client_instance.query.call_args[0][0]
    assert "CREATE OR REPLACE TABLE" in query_arg
    assert "SELECT" in query_arg
    assert "JOIN" in query_arg
    assert "MA5" in query_arg  # 移動平均列が含まれているか
