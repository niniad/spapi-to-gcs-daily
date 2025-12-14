"""
Orders API Module

このモジュールは、SP-APIのOrders API (getOrders) を使用して注文データを取得し、JSONL形式でGCSに保存します。
Restricted Data Token (RDT) を使用して、BuyerInfo (BuyerEmailなど) を含む詳細データを取得します。

- 取得期間: 過去8日前〜1日前 (設定可能)
- 保存形式: JSONL (BigQuery等での読み込みに最適)
- ファイル命名: orders-api/YYYYMMDD.jsonl
"""

import json
import time
from datetime import datetime, timedelta, timezone
from google.cloud import storage
from utils.sp_api_auth import get_restricted_data_token
from utils.http_retry import request_with_retry


# ===================================================================
# 設定
# ===================================================================
MARKETPLACE_ID = "A1VC38T7YXB528"  # 日本
START_DAYS_AGO = 8
END_DAYS_AGO = 1
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
GCS_BUCKET_NAME = "sp-api-bucket"
GCS_FILE_PREFIX = "orders-api/"

# API パス
ORDERS_API_PATH = "/orders/v0/orders"


def _upload_to_gcs(bucket_name, blob_name, content):
    """
    GCSにファイルをアップロードします。
    
    Args:
        bucket_name: GCSバケット名
        blob_name: 保存するファイル名
        content: ファイルの内容 (JSONL文字列)
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content, content_type='application/x-ndjson') # JSONLのMIMEタイプ
        print(f"  -> GCSへの保存成功: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f"  -> Error: GCSへのアップロードに失敗しました: {e}")


def _fetch_orders_for_date(date_str, rdt_token):
    """
    指定された日付の注文データをページネーション込みで全件取得します。
    """
    orders = []
    next_token = None
    
    # 期間設定: 指定日の 00:00:00 〜 23:59:59 (JSTベースで処理したいが、APIはISO8601 UTC)
    # ここではシンプルに LastUpdatedAfter / Before を使う。
    # APIの仕様上、LastUpdatedAfterのタイムゾーンに注意が必要。
    # 簡易的に UTC で指定日全体をカバーする。
    start_ts = f"{date_str}T00:00:00Z"
    end_ts = f"{date_str}T23:59:59Z"

    print(f"  -> 注文データ取得中 ({start_ts} 〜 {end_ts})...")

    while True:
        params = {
            "MarketplaceIds": [MARKETPLACE_ID],
            "LastUpdatedAfter": start_ts,
            "LastUpdatedBefore": end_ts,
            # "CreatedAfter": ... # CreatedAfterを使うかLastUpdatedAfterを使うかは要件次第。
            # 今回はステータス変更も拾いたいのでLastUpdatedが推奨だが、
            # 日次バッチとしては作成日ベースが良い場合もある。一旦LastUpdatedで実装。
            # "Assignments": [], # 必要に応じて追加
        }
        if next_token:
            params = {"NextToken": next_token} # NextTokenがある場合はそれだけ送るのが一般的

        headers = {
            'x-amz-access-token': rdt_token,
            'Content-Type': 'application/json'
        }

        try:
            response = request_with_retry(
                'GET',
                f"{SP_API_ENDPOINT}{ORDERS_API_PATH}",
                headers=headers,
                params=params
            )
            
            data = response.json()
            payload = data.get("payload", {})
            fetched_orders = payload.get("Orders", [])
            orders.extend(fetched_orders)
            
            print(f"    -> {len(fetched_orders)} 件取得 (Total: {len(orders)})")

            next_token = payload.get("NextToken")
            if not next_token:
                break
            
            time.sleep(1) # レートリミット考慮

        except Exception as e:
            print(f"    -> Error: APIリクエスト失敗: {e}")
            break

    return orders


def run():
    """
    Orders API (getOrders) の実行メイン関数
    """
    print("\n=== Orders API (JSONL) 処理開始 ===")
    
    try:
        # RDT (Restricted Data Token) を取得
        # buyerInfo, shippingAddress へのアクセス権を含める
        rdt = get_restricted_data_token(
            path=ORDERS_API_PATH,
            method='GET',
            data_elements=['buyerInfo', 'shippingAddress'] 
        )
        
        # データ取得期間を計算
        utc_now = datetime.now(timezone.utc)
        start_date = utc_now - timedelta(days=START_DAYS_AGO)
        end_date = utc_now - timedelta(days=END_DAYS_AGO)
        print(f"データ取得期間: {start_date.strftime('%Y-%m-%d')} から {end_date.strftime('%Y-%m-%d')}")
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\n[{date_str}] の処理を開始...")

            orders = _fetch_orders_for_date(date_str, rdt)
            
            if orders:
                # JSONL形式に変換
                # 各行が1つのJSONオブジェクトになる形式
                jsonl_lines = [json.dumps(order, ensure_ascii=False) for order in orders]
                jsonl_content = "\n".join(jsonl_lines)
                
                if jsonl_content:
                    blob_name = f"{GCS_FILE_PREFIX}{current_date.strftime('%Y%m%d')}.jsonl"
                    _upload_to_gcs(GCS_BUCKET_NAME, blob_name, jsonl_content)
            else:
                print("  -> 注文データなし。スキップ。")

            current_date += timedelta(days=1)
            time.sleep(2)

        print("\n=== Orders API (JSONL) 処理完了 ===")

    except Exception as e:
        print(f"Error: Orders API 処理中に致命的なエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run()
