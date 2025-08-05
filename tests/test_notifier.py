"""
notifier モジュールのテスト

- Slack通知（notify_slack）が requests.post を正しく呼び出すか
- Slackメッセージの整形（format_slack_message）が期待通りの出力になるか
- 実際のSlack送信は行わず、Mock による確認のみを行う
"""

from unittest import mock
from utils.notify.notifier import notify_slack, format_slack_message


@mock.patch("utils.notify.notifier.requests.post")
def test_notify_slack_success(mock_post):
    """
    notify_slack() が 成功メッセージを正しく送信するかを検証する。
    """
    # requests.post の戻り値をモック設定
    mock_response = mock.Mock()
    mock_response.raise_for_status.return_value = None
    mock_post.return_value = mock_response

    # 環境変数と関数呼び出し
    with mock.patch.dict(
        "os.environ",
        {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/services/test"},
    ):
        notify_slack("成功メッセージ", success=True)

    # 検証：POSTが呼ばれたか
    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert kwargs["json"]["attachments"][0]["color"] == "#2eb886"
    assert (
        "成功メッセージ"
        in kwargs["json"]["attachments"][0]["fields"][0]["value"]
    )


@mock.patch("utils.notify.notifier.requests.post")
def test_notify_slack_error(mock_post):
    """
    notify_slack() が エラーメッセージを正しく送信するかを検証する。
    """
    # requests.post の戻り値をモック設定
    mock_response = mock.Mock()
    mock_response.raise_for_status.return_value = None
    mock_post.return_value = mock_response

    # 環境変数と関数呼び出し
    with mock.patch.dict(
        "os.environ",
        {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/services/test"},
    ):
        notify_slack("エラーメッセージ", success=False)

    # 検証：POSTが呼ばれたか
    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert kwargs["json"]["attachments"][0]["color"] == "#ff0000"
    assert (
        "エラーメッセージ"
        in kwargs["json"]["attachments"][0]["fields"][0]["value"]
    )


def test_format_slack_message_success():
    """
    format_slack_message() が 成功メッセージを正しく整形するかを検証する。
    """
    payload = {
        "status": "success",
        "message": "ETL succeeded",
        "mode": "extract",
        "timestamp": "2025-08-05T12:34:56",
    }

    # 整形結果の検証
    msg = format_slack_message(payload)
    assert "✅ ETL succeeded" in msg
    assert "実行モード" in msg
    assert "2025-08-05" in msg


def test_format_slack_message_error():
    """
    format_slack_message() が エラー詳細を含めて整形するかを検証する。
    """
    payload = {
        "status": "error",
        "message": "ETL failed",
        "mode": "extract",
        "timestamp": "2025-08-05T12:34:56",
        "error_message": "Timeout occurred",
    }

    # 整形結果の検証
    msg = format_slack_message(payload)
    assert "❌ ETL failed" in msg
    assert "error: Timeout occurred" in msg
