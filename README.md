# 株価ETLパイプライン構築プロジェクト

## 概要

このプロジェクトは、`yfinance` を使用して株価データを取得し、ETL処理（Extract → Transform → Load）を通じて GCP 環境にデータ保存・通知・可視化を実現したものです。

Cloud Functions / BigQuery / Cloud Storage / Scheduler を用いた自動ETL構成、および Slack通知やロガーによる可観測性の設計により、データエンジニアリングにおける基盤構築スキルを実践形式で示しています。

---

## プロジェクト全体の解説資料（スライド形式）

本プロジェクトの全体像・構成・可視化例をまとめたプレゼン資料です。

🔗 [【2025年8月】青木大和ポートフォリオ（スライド資料）](https://drive.google.com/file/d/1L99E64LngO7qMrIsZxDR7_KjWF_rY-bi/view) 
※Looker Studioによる可視化例も含まれています。

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

| テスト対象モジュール                   | 主な検証内容 |
|----------------------------------|-------------------------------|
| `fetch_stock_prices`             | APIから取得したデータ構造・件数・例外処理の検証 |
| `format_stock_prices`            | 整形処理後のデータ構造・型・日付変換の確認     |
| `save_json_to_gcs`               | GCSアップロード処理の呼び出し確認（mock使用） |
| `load_to_bigquery`               | 一時テーブルへのロード／MERGE処理の確認       |
| `transform_to_analytics_table`   | JOIN後の非正規化データの構造・指標計算の確認 |
| `notifier`                       | Slack通知の送信成功／失敗パターンの検証       |
| `logger`                         | Cloud Logging／GCSログ出力のmock検証         |
| `pipeline (run_extract)`         | ETLステップが想定通り呼び出されるかの確認     |
| `pipeline (run_extract_range)`   | 日付ループ処理の呼び出し回数や連携確認       |
| `main.py`（Cloud Functions起点） | リクエスト入力に応じた分岐処理・異常時ログ確認（mock）|

> ※ `main.py` を含め、Cloud SDK や外部API／GCS／BigQuery などへの依存処理はすべて mock によってテスト対象とし、ローカル環境で完結できる形で検証しています。


---

## ログ設計と通知

| 出力先           | 目的         | 備考                              |
| ------------- | ---------- | ------------------------------- |
| Slack通知       | 成功/失敗の即時通知 | 成功/失敗ステータス + 概要メッセージを表示         |
| GCSログ         | 処理記録の保存    | 日付単位でJSONファイルをアーカイブ             |
| Cloud Logging | 詳細なステップログ  | functions-framework や GCP上で自動出力 |

> 粒度と用途を分離して、運用面での可観測性と調査性を確保。ローカルログ出力は現在は未使用です。

---

## Dataform プロジェクトについて

このプロジェクトで実施している「分析用データへの変換（整形）」部分 [`transform_to_analytics.py`](https://github.com/yamato-aoki/yfinance_gcp_project2/blob/main/utils/etl/transform_to_analytics.py) を、今後は別プロジェクトに分けて管理する予定です。  

理由は、**ETLと分析用整形処理を分けることで、管理や改修がしやすくなるため** です。  
変換処理には Google Cloud の **Dataform** を利用し、SQLのバージョン管理やテーブル作成を自動化します。  

- Dataform版の変換処理: [`yfinance-dataform`](https://github.com/yamato-aoki/yfinance-dataform)  

これにより、ETL基盤と分析基盤の役割を明確に分け、保守性と拡張性を高めています。  
**実行トリガーについては、Cloud Functions 経由での Dataform API 呼び出しや、必要に応じて Cloud Dataflow の活用を検討中です。**

---

- 目的：ETL基盤と分析整形の責務分離
- 主な機能：集計テーブル作成、変換処理のバージョン管理
- 備考：別リポジトリはポートフォリオ面接時に共有可能


---

## 作者情報

- 名前：青木 大和（Yamato Aoki）
- 志望職種：データエンジニア
- 注力している領域：データ基盤構築 / ETL / GCP活用 / 再現性ある設計

---

## 注意事項

このリポジトリは学習・ポートフォリオ提出目的で作成されたものであり、商用利用・再配布はご遠慮ください。

