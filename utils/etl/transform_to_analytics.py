"""
分析用の非正規化テーブルを BigQuery 上に作成するモジュール。

- 正規化テーブル（stock_prices）とマスタ（tickers, sectors, currencies）を結合
- 終値・出来高・移動平均・騰落率・勝率フラグなどを含む集約済テーブルを出力
- テーブル名：yfinance_analytics.stock_prices_analysis
"""

# utils/etl/transform_to_analytics.py

# --- 標準ライブラリ ---
import logging

# --- サードパーティ ---
from google.cloud import bigquery

# --- ロガー設定 ---
logger = logging.getLogger(__name__)


def transform_to_analytics_table() -> None:
    """
    非正規化テーブル（stock_prices_analysis）を作成する。

    正規化テーブル stock_prices とマスタ（tickers, sectors, currencies）を結合し、
    以下の指標を含んだ分析用テーブルを CREATE OR REPLACE で上書き生成する：

    - 会社名、業種名、通貨名（マスタJOIN）
    - 終値・出来高（基本指標）
    - 移動平均（MA5, MA25, MA75）
    - 騰落率（前日比変化率）
    - 勝率フラグ（前日より上昇なら1）

    実行成功時は info ログ、失敗時は error ログを出力。
    """
    client = bigquery.Client()

    # 非正規化テーブル作成クエリ
    create_sql = """
    CREATE OR REPLACE TABLE `yfinance_analytics.stock_prices_analysis` AS
    SELECT
        sp.ticker_id,
        t.company_name,
        s.sector_name,
        c.currency_name,
        sp.date,
        sp.close_price,
        sp.volume,

        -- 移動平均（直近5/25/75営業日）
        ROUND(AVG(sp.close_price) OVER (
            PARTITION BY sp.ticker_id ORDER BY sp.date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ), 4) AS MA5,
        ROUND(AVG(sp.close_price) OVER (
            PARTITION BY sp.ticker_id ORDER BY sp.date ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
        ), 4) AS MA25,
        ROUND(AVG(sp.close_price) OVER (
            PARTITION BY sp.ticker_id ORDER BY sp.date ROWS BETWEEN 74 PRECEDING AND CURRENT ROW
        ), 4) AS MA75,

        -- 騰落率（前日比）
        ROUND(
            SAFE_DIVIDE(
                sp.close_price - LAG(sp.close_price) OVER (PARTITION BY sp.ticker_id ORDER BY sp.date),
                LAG(sp.close_price) OVER (PARTITION BY sp.ticker_id ORDER BY sp.date)
            ),
            4
        ) AS change_rate,

        -- 勝率フラグ（前日比で上昇していれば1）
        CASE
            WHEN SAFE_DIVIDE(
                sp.close_price - LAG(sp.close_price) OVER (PARTITION BY sp.ticker_id ORDER BY sp.date),
                LAG(sp.close_price) OVER (PARTITION BY sp.ticker_id ORDER BY sp.date)
            ) > 0 THEN 1
            ELSE 0
        END AS is_win

    FROM
        `yfinance_analytics.stock_prices` sp
    LEFT JOIN
        `yfinance_analytics.tickers` t ON sp.ticker_id = t.ticker_id
    LEFT JOIN
        `yfinance_analytics.sectors` s ON t.sector_id = s.sector_id
    LEFT JOIN
        `yfinance_analytics.currencies` c ON t.currency_id = c.currency_id;
    """

    logger.info(
        "[start] transform_to_analytics_table: Starting to create denormalized table."
    )

    try:
        query_job = client.query(create_sql)
        query_job.result()
        logger.info(
            "[success] transform_to_analytics_table: Successfully created denormalized table: stock_prices_analysis"
        )
    except Exception as e:
        logger.error(
            f"[error] transform_to_analytics_table: Failed to create denormalized table. Error: {e}"
        )
        raise
