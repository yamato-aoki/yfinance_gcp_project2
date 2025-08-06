# 株価ETLパイプライン構築プロジェクト

## 概要

このプロジェクトは、`yfinance` を使用して株価データを取得し、ETL処理（Extract → Transform → Load）を通じて GCP 環境にデータ保存・通知・可視化を実現したものです。

Cloud Functions / BigQuery / Cloud Storage / Scheduler を用いた自動ETL構成、および Slack通知やロガーによる可観測性の設計により、データエンジニアリングにおける基盤構築スキルを実践形式で示しています。

---

## 使用技術

| 項目       | 技術・サービス                           |
| -------- | --------------------------------- |
| データ取得    | Python, yfinance                  |
| ETL処理    | Cloud Functions（Gen2）             |
| スケジューリング | Cloud Scheduler                   |
| データ保存    | Cloud Storage（.ndjson） + BigQuery |
| 通知       | Slack Webhook                     |
| ログ管理     | Cloud Logging + Cloud Storage     |
| 可視化      | Looker Studio（BigQuery上の非正規化テーブル） |

---

## ディレクトリ構成

```
yfinance_gcp_project2/
├── main.py                    # Cloud Functions エントリポイント
├── requirements.txt           # 依存パッケージ定義
├── .env                       # ローカル環境変数（Gitには含めない）
│
├── handlers/
│   └── request_handler.py     # modeによる処理ルーティング
│
├── utils/
│   ├── etl/                   # ETL処理（抽出・整形・保存・ロード）
│   ├── init/                  # 初期マスタ登録
│   ├── notify/                # Slack通知処理
│   ├── logger.py              # ログ出力（Cloud Logging / GCS）
│   └── pipeline.py            # 一括処理実行
│
├── master/                    # tickers / sectors / currencies 定義
├── schema/                    # BigQuery用 JSON スキーマ
├── tests/                     # 単体テスト（pytest）
```

---

## 実行方法（GCP環境向け）

このプロジェクトは GCP 上での運用を前提とした構成です。以下は、Cloud Functions などを使って再現・動作確認を行う流れです。

### 1. 仮想環境の作成（任意・ローカル検証用途）

```bash
python -m venv venv
source venv/bin/activate  # Windowsなら venv\Scripts\activate
pip install -r requirements.txt
```

### 2. `.env` ファイルを作成（ローカル実行時のみ）

以下の環境変数を設定します：

```
GCS_BUCKET_NAME=your-bucket-name
BIGQUERY_DATASET_ID=your_dataset
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXXXX
```

### 3. 実行（Cloud Functions ローカル or デプロイ）

#### Cloud Functions ローカル実行（推奨）

```bash
functions-framework --target=etl_dispatcher
```

> `.http` や curl で mode を指定して実行可能です。例：

```bash
curl -X POST http://localhost:8080 -H "Content-Type: application/json" -d '{"mode": "etl"}'
```

> ※再現には GCP リソース（Cloud Storage バケットや BigQuery テーブル）が事前に作成・権限設定されている必要があります。

---

## 主な機能

- 指定銘柄の株価データ取得（`yfinance` 使用）
- 日別ファイル（.ndjson）として GCS に保存
- BigQuery へのロード（MERGE戦略によるUpsert対応）
- 分析用の非正規化テーブルも自動生成
- Slack への成功/失敗通知
- GCS/Cloud Logging の2重ログ設計
- Looker Studio による可視化（別途参照）

---

## テストカバレッジ

主要モジュールに対して、`pytest` + `mock` による単体テストを実施しています。

| テスト対象                         | 内容                        |
| ----------------------------- | ------------------------- |
| fetch\_stock\_prices          | APIからの取得データが構造的に正しいかを確認   |
| format\_stock\_prices         | 整形後データの構造・データ型の検証         |
| save\_json\_to\_gcs           | GCSアップロードの呼び出しをmockで確認    |
| pipeline（run\_extract）        | ETLステップ呼び出しが想定通り行われているか   |
| pipeline（run\_extract\_range） | 日付ループ処理が適切に機能するか（回数などを検証） |

> `main.py`（Cloud Functionsエントリ）は本番環境での実行を想定。単体テスト対象外。

---

## ログ設計と通知

| 出力先           | 目的         | 備考                              |
| ------------- | ---------- | ------------------------------- |
| Slack通知       | 成功/失敗の即時通知 | 成功/失敗ステータス + 概要メッセージを表示         |
| GCSログ         | 処理記録の保存    | 日付単位でJSONファイルをアーカイブ             |
| Cloud Logging | 詳細なステップログ  | functions-framework や GCP上で自動出力 |

> 粒度と用途を分離して、運用面での可観測性と調査性を確保。ローカルログ出力は現在は未使用です。

---

## 成果物の活用（プレゼン資料）

本プロジェクトの背景・構成・工夫点・可視化結果などをまとめたプレゼン資料を以下に掲載しています。

🔗 [【2025年8月】青木大和ポートフォリオ（スライド資料）](https://bit.ly/3UbUZpL) ※Looker Studioによる可視化例も含まれています。

---

## 作者情報

- 名前：青木 大和（Yamato Aoki）
- 志望職種：データエンジニア
- 得意領域：データ基盤構築 / ETL / 可視化 / GCP活用 / 再現性ある設計

---

## 注意事項

このリポジトリは学習・ポートフォリオ提出目的で作成されたものであり、商用利用・再配布はご遠慮ください。

