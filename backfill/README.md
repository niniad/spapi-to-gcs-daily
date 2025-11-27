# Historical Data Backfill - 実行ガイド

このディレクトリには、SP-APIから過去データを**ローカル環境に取得するため**のバックフィルスクリプトが含まれています。
これらのスクリプトはデータを取得してローカルディスクに保存する機能のみを持ち、GCPへのアップロードは行いません。

## 📁 ディレクトリ構成

```
backfill/
├── config/
│   ├── credentials.json.template  # 認証情報テンプレート
│   └── credentials.json           # 認証情報（ユーザーが作成）
├── scripts/
│   ├── auth.py                    # 認証モジュール
│   ├── backfill_brand_analytics_monthly.py
│   ├── backfill_brand_analytics_weekly.py
│   ├── backfill_catalog_items.py
│   ├── backfill_fba_inventory.py
│   ├── backfill_ledger_detail.py
│   ├── backfill_ledger_summary.py
│   └── backfill_transactions.py
└── data/                          # 取得したデータの保存先
    ├── brand-analytics/
    │   ├── WEEK/
    │   └── MONTH/
    ├── catalog-items/
    ├── fba-inventory/
    ├── ledger-detail/
    ├── ledger-summary/
    └── transactions/
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
```

---

## 🚀 バックフィル実行コマンド（ローカル取得）

以下のスクリプトを順次実行してデータをローカルに取得してください。

### 1. Ledger Summary (月次)

過去18ヶ月分のLedger Summaryレポートを月次で取得します。

```powershell
py backfill/scripts/backfill_ledger_summary.py
```

**取得データ**: 先月から過去18ヶ月分
**保存先**: `backfill/data/ledger-summary/YYYYMM.tsv`
**所要時間**: 約15〜30分

### 2. Ledger Detail (月次)

過去18ヶ月分のLedger Detailレポートを月次で取得します。

```powershell
py backfill/scripts/backfill_ledger_detail.py
```

**取得データ**: 先月から過去18ヶ月分
**保存先**: `backfill/data/ledger-detail/YYYYMM.tsv`
**所要時間**: 約30〜60分

### 3. Transactions (日次)

過去18ヶ月分のトランザクションデータを日次で取得します。

```powershell
py backfill/scripts/backfill_transactions.py
```

**取得データ**: 昨日から過去18ヶ月分
**保存先**: `backfill/data/transactions/YYYYMMDD.json`
**所要時間**: 数時間（データ量に依存）

### 4. Brand Analytics (週次・月次)

過去2年分のBrand Analyticsレポートを取得します。

**週次レポート**:
```powershell
py backfill/scripts/backfill_brand_analytics_weekly.py
```
**保存先**: `backfill/data/brand-analytics/WEEK/YYYYMMDD-YYYYMMDD.json`

**月次レポート**:
```powershell
py backfill/scripts/backfill_brand_analytics_monthly.py
```
**保存先**: `backfill/data/brand-analytics/MONTH/YYYYMM.json`

### 5. FBA Inventory (スナップショット)

**現在**のFBA在庫情報を取得します（過去データは取得不可）。

```powershell
py backfill/scripts/backfill_fba_inventory.py
```

**保存先**: `backfill/data/fba-inventory/YYYYMMDD.json`

### 6. Catalog Items (スナップショット)

**現在**の全ASINのカタログ情報を取得します（過去データは取得不可）。
※ FBA InventoryのデータからASINリストを生成するため、先にFBA Inventoryを実行してください。

```powershell
py backfill/scripts/backfill_catalog_items.py
```

**保存先**: `backfill/data/catalog-items/catalog_item_{ASIN}_{DATE}.json`
**所要時間**: ASIN数 × 0.5秒 + α

---

## ⚠️ 注意事項

### レート制限とリトライ
- 各スクリプトはSP-APIのレート制限を考慮して設計されています。
- 429エラー（Too Many Requests）発生時は自動的に待機・リトライします。

### 中断と再開
- すべてのスクリプトは中断しても安全です（Ctrl+C）。
- 既に取得済みのファイルは自動的にスキップされるため、再実行時は未取得分のみ処理されます。

## 📞 サポート
問題が発生した場合は、エラーメッセージと実行したスクリプト名を記録して報告してください。
