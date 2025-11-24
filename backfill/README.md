# Historical Data Backfill - 実行ガイド

このディレクトリには、SP-APIから過去データを取得するためのバックフィルスクリプトが含まれています。

## 📁 ディレクトリ構成

```
backfill/
├── config/
│   ├── credentials.json.template  # 認証情報テンプレート
│   └── credentials.json           # 認証情報（ユーザーが作成）
├── scripts/
│   ├── auth.py                    # 認証モジュール
│   ├── backfill_brand_analytics.py
│   ├── backfill_ledger_detail.py
│   └── backfill_ledger_summary.py
└── data/                          # 取得したデータの保存先
    ├── brand-analytics/
    │   ├── WEEK/
    │   └── MONTH/
    ├── ledger-detail/
    └── ledger-summary/
```

## 🔐 セットアップ

### 1. 認証情報の設定

`backfill/config/credentials.json` を作成し、以下の形式でSP-API認証情報を記入してください。

```json
{
  "refresh_token": "YOUR_REFRESH_TOKEN_HERE",
  "client_id": "YOUR_CLIENT_ID_HERE",
  "client_secret": "YOUR_CLIENT_SECRET_HERE"
}
```

**テンプレートをコピーして使用する場合**:
```powershell
Copy-Item backfill/config/credentials.json.template backfill/config/credentials.json
```

その後、`credentials.json` を編集して実際の認証情報を記入してください。

### 2. 認証のテスト

認証情報が正しく設定されているか確認します。

```powershell
py backfill/scripts/auth.py
```

成功すると以下のように表示されます:
```
認証情報を読み込んでいます...
✓ 認証情報の読み込み成功
  Client ID: amzn1.appl...

アクセストークンを取得しています...
✓ アクセストークンの取得成功
  Token: Atza|IwEBIJK3Uj...
```

## 🚀 実行方法

### Brand Analytics (週次・月次)

過去2年分のBrand Analyticsレポートを取得します。

```powershell
py backfill/scripts/backfill_brand_analytics.py
```

**取得データ**:
- 週次データ: 直近の完全な週から過去2年分
- 月次データ: 先月から過去2年分

**保存先**:
- `backfill/data/brand-analytics/WEEK/YYYYMMDD-YYYYMMDD.json`
- `backfill/data/brand-analytics/MONTH/YYYYMM.json`

**所要時間**: 約30分〜1時間（データ量とAPI応答速度による）

---

### Ledger Detail (日次)

過去18ヶ月分のLedger Detailレポートを日次で取得します。

```powershell
py backfill/scripts/backfill_ledger_detail.py
```

**取得データ**:
- 昨日から過去18ヶ月分（約540日）

**保存先**:
- `backfill/data/ledger-detail/YYYYMMDD.tsv`

**所要時間**: 約3〜5時間（540日分のデータ取得）

**注意**: 
- 処理時間が長いため、途中で中断した場合でも既に取得済みのファイルはスキップされます。
- 50日ごとに進捗状況が表示されます。

---

### Ledger Summary (月次)

過去18ヶ月分のLedger Summaryレポートを月次で取得します。

```powershell
py backfill/scripts/backfill_ledger_summary.py
```

**取得データ**:
- 先月から過去18ヶ月分

**保存先**:
- `backfill/data/ledger-summary/YYYYMM.tsv`

**所要時間**: 約15〜30分

---

## 📊 データのGCSアップロード

バックフィルスクリプトで取得したデータは、手動でGCSにアップロードしてください。

### アップロード先

| ローカルディレクトリ | GCSアップロード先 |
|-------------------|-----------------|
| `backfill/data/brand-analytics/WEEK/` | `gs://sp-api-bucket/brand-analytics-search-query-performance-report/WEEK/` |
| `backfill/data/brand-analytics/MONTH/` | `gs://sp-api-bucket/brand-analytics-search-query-performance-report/MONTH/` |
| `backfill/data/ledger-detail/` | `gs://sp-api-bucket/ledger-detail-view-data/` |
| `backfill/data/ledger-summary/` | `gs://sp-api-bucket/ledger-summary-view-data/` |

### アップロードコマンド例

```bash
# Brand Analytics - WEEK
gsutil -m cp backfill/data/brand-analytics/WEEK/*.json gs://sp-api-bucket/brand-analytics-search-query-performance-report/WEEK/

# Brand Analytics - MONTH
gsutil -m cp backfill/data/brand-analytics/MONTH/*.json gs://sp-api-bucket/brand-analytics-search-query-performance-report/MONTH/

# Ledger Detail
gsutil -m cp backfill/data/ledger-detail/*.tsv gs://sp-api-bucket/ledger-detail-view-data/

# Ledger Summary
gsutil -m cp backfill/data/ledger-summary/*.tsv gs://sp-api-bucket/ledger-summary-view-data/
```

## ⚠️ 注意事項

### レート制限

- 各スクリプトはSP-APIのレート制限を考慮して設計されています
- リクエスト間に2秒の待機時間を設定
- 429エラー発生時は自動的にリトライ（最大5〜10回）

### 中断と再開

- すべてのスクリプトは中断しても安全です（Ctrl+C）
- 既に取得済みのファイルは自動的にスキップされます
- 再実行すると、未取得のデータのみ取得します

### エラーハンドリング

- データが存在しない期間はスキップされます
- エラーが発生した場合はログに記録され、次の期間へ進みます
- 致命的なエラーが発生した場合はスタックトレースが表示されます

## 🔍 トラブルシューティング

### 認証エラー

```
FileNotFoundError: 認証情報ファイルが見つかりません
```
→ `backfill/config/credentials.json` を作成してください。

```
ValueError: 認証情報ファイルに 'refresh_token' が含まれていません
```
→ `credentials.json` の形式を確認してください。

### データ取得エラー

```
レポート処理失敗 (Status: FATAL)
```
→ SP-APIの一時的な問題の可能性があります。しばらく待ってから再実行してください。

```
タイムアウト
```
→ レポート生成に時間がかかっています。スクリプトは自動的に次の期間へ進みます。

## 📝 ログの確認

各スクリプトは実行中にコンソールに進捗状況を表示します。

**Brand Analytics**:
```
=== 週次データのバックフィル開始 ===
  [取得中] 20231119-20231125.json
    ✓ 保存完了
  [SKIP] 20231112-20231118.json (既存)
...
週次データ完了: 成功 104件, スキップ 0件
```

**Ledger Detail**:
```
=== Ledger Detail データのバックフィル開始 ===
  [1] 20251123.tsv
    ✓ 保存完了
  [2] 20251122.tsv
    ✓ 保存完了
...
  進捗: 50日処理済み (成功: 48, スキップ: 2)
```

## 🎯 推奨実行順序

1. **Brand Analytics** (約30分〜1時間)
2. **Ledger Summary** (約15〜30分)
3. **Ledger Detail** (約3〜5時間) ← 最も時間がかかる

Ledger Detailは最後に実行することをお勧めします。

## 📞 サポート

問題が発生した場合は、以下の情報を含めて報告してください:
- 実行したスクリプト名
- エラーメッセージ
- 実行環境（Python バージョン、OS）
