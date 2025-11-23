# SP-API to GCS Daily

SP-APIから各種レポートを取得し、Google Cloud Storage (GCS) に保存するCloud Run Functionsです。

## 手動実行 / テスト方法

Google Cloud Shell（または `gcloud` コマンドが設定されたターミナル）から以下のコマンドを実行することで、各エンドポイントを手動でトリガーできます。

### 1. 全エンドポイントの実行（デフォルト）
すべてのレポート取得処理を順次実行します。

```bash
curl -X POST "https://spapi-to-gcs-daily-850116866513.asia-northeast1.run.app" \
-H "Authorization: bearer $(gcloud auth print-identity-token)"
```

### 2. Sales and Traffic Report のみ実行
Sales and Traffic Report の処理のみを実行します。

```bash
curl -X POST "https://spapi-to-gcs-daily-850116866513.asia-northeast1.run.app?endpoint=sales_and_traffic" \
-H "Authorization: bearer $(gcloud auth print-identity-token)"
```

### 3. Settlement Report のみ実行
Settlement Report の処理のみを実行します。

```bash
curl -X POST "https://spapi-to-gcs-daily-850116866513.asia-northeast1.run.app?endpoint=settlement_report" \
-H "Authorization: bearer $(gcloud auth print-identity-token)"
```

### 4. Brand Analytics Search Query Performance Report のみ実行
Brand Analytics Search Query Performance Report の処理のみを実行します。

```bash
curl -X POST "https://spapi-to-gcs-daily-850116866513.asia-northeast1.run.app?endpoint=brand_analytics_report" \
-H "Authorization: bearer $(gcloud auth print-identity-token)"
```

### 5. Ledger Detail View Data Report のみ実行
Ledger Detail View Data Report の処理のみを実行します。

```bash
curl -X POST "https://spapi-to-gcs-daily-850116866513.asia-northeast1.run.app?endpoint=ledger_detail" \
-H "Authorization: bearer $(gcloud auth print-identity-token)"
```

### 6. Ledger Summary View Data Report のみ実行
Ledger Summary View Data Report の処理のみを実行します。

```bash
curl -X POST "https://spapi-to-gcs-daily-850116866513.asia-northeast1.run.app?endpoint=ledger_summary" \
-H "Authorization: bearer $(gcloud auth print-identity-token)"
```

---
**Note:**
- `$(gcloud auth print-identity-token)` は、現在ログインしているユーザーまたはサービスアカウントのIDトークンを自動的に取得してヘッダーに設定します。
- Cloud Run Functionsの認証設定で「認証が必要」となっている場合に必要です。
