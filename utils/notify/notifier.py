"""
Slack通知を整形・送信する関数を提供するモジュール。

- notify_slack(): Slackへのメッセージ送信
- format_slack_message(): JSONログをSlack用テキストに整形
"""

# utils/notify/notifier.py

# --- 標準ライブラリ ---
import logging
import os

# --- サードパーティ ---
import requests

# --- ロガー設定 ---
logger = logging.getLogger(__name__)


def notify_slack(message: str, success: bool = True) -> None:
    """
    指定メッセージをSlackに通知する。

    Args:
        message (str): 通知メッセージ本文（整形済み）
        success (bool): 成功通知かエラー通知か（色分けのため）

    Returns:
        None

    Raises:
        ValueError: 環境変数 SLACK_WEBHOOK_URL が未設定の場合
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        raise ValueError(
            "SLACK_WEBHOOK_URL is not set in environment variables."
        )

    logger.info("[start] notify_slack: Starting to send Slack message.")

    color = "#2eb886" if success else "#ff0000"  # 緑 or 赤

    payload = {
        "attachments": [
            {
                "fallback": message,
                "color": color,
                "fields": [
                    {
                        "title": "yfinance-notifier",
                        "value": message,
                        "short": False,
                    }
                ],
            }
        ]
    }

    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        logger.info("[success] notify_slack: Successfully sent Slack message.")
    except Exception as e:
        logger.error(
            f"[error] notify_slack: Failed to send Slack message. Error: {str(e)}"
        )


def format_slack_message(payload: dict) -> str:
    """
    JSON形式のログをSlack通知用のテキストに整形する。

    成功/失敗のステータスに応じて、絵文字と内容を変更。
    エラー時には error_message も含める。

    Args:
        payload (dict): Slack通知用に整形対象となるログ情報

    Returns:
        str: 整形済みSlackメッセージ
    """
    status_icon = "✅" if payload.get("status") == "success" else "❌"
    message_lines = [
        f"{status_icon} {payload.get('message', '処理メッセージなし')}",
        f"実行モード: {payload.get('mode', '-')}",
        f"実行時刻: {payload.get('timestamp', '-')}",
    ]

    if payload.get("status") == "error":
        error_msg = payload.get("error_message", "エラー詳細なし")
        message_lines.append(f"error: {error_msg}")

    return "\n".join(message_lines)
