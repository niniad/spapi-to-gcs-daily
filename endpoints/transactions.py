"""
Transactions API Endpoint

このモジュールは、Amazon SP-APIのTransactions APIからトランザクションデータを取得し、
GCSに保存します。

API仕様:
- エンドポイント: GET /finances/2024-06-19/transactions
- ページネーション: nextTokenを使用
- データ形式: JSON (NDJSON形式で保存)
"""

import json
import time
from datetime import datetime, timedelta, timezone
from google.cloud import storage
from utils.sp_api_auth import get_access_token
from utils.http_retry import request_with_retry


# ===================================================================
# 設定
# ===================================================================
MARKETPLACE_ID = "A1VC38T7YXB528"  # 日本
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
GCS_BUCKET = "sp-api-bucket"
GCS_PREFIX = "transactions/"


def get_posted_date_range():
    """
    取得対象の日付範囲を計算します。
    日本時間の昨日に対して、00:00:00から23:59:59までを取得します。
    例: 日本時間2025-06-17 → postedAfter=2025-06-17T00:00:00.00+09:00, postedBefore=2025-06-17T23:59:59.00+09:00
    
    Returns:
        tuple: (posted_after, posted_before, jst_date_str) のタプル
    """
    utc_now = datetime.now(timezone.utc)
    # 日本時間の昨日を計算
    jst_now = utc_now + timedelta(hours=9)
    jst_yesterday = jst_now - timedelta(days=1)
    jst_date_str = jst_yesterday.strftime('%Y-%m-%d')
    
    # 日本時間で00:00:00から23:59:59まで
    posted_after = f"{jst_date_str}T00:00:00.00+09:00"
    posted_before = f"{jst_date_str}T23:59:59.00+09:00"
    
    return (posted_after, posted_before, jst_date_str)


def fetch_transactions(posted_after, posted_before, headers):
    """
    Transactions APIからすべてのトランザクションを取得します。
    
    Args:
        posted_after: 開始日時 (ISO 8601)
        posted_before: 終了日時 (ISO 8601)
        headers: HTTPヘッダー
        
    Returns:
        list: トランザクションのリスト
    """
    all_transactions = []
    next_token = None
    page_count = 0
    
    while True:
        page_count += 1
        print(f"  ページ {page_count} を取得中...")
        
        # クエリパラメータ構築
        params = {
            'postedAfter': posted_after,
            'postedBefore': posted_before,
            'marketplaceId': MARKETPLACE_ID
        }
        
        if next_token:
            params['nextToken'] = next_token
        
        # APIリクエスト
        url = f"{SP_API_ENDPOINT}/finances/2024-06-19/transactions"
        response = request_with_retry(
            'GET',
            url,
            headers=headers,
            params=params,
            max_retries=10,
            retry_delay=60
        )
        
        data = response.json()
        
        # トランザクションを追加
        if 'payload' in data and 'transactions' in data['payload']:
            transactions = data['payload']['transactions']
            all_transactions.extend(transactions)
            print(f"    {len(transactions)}件のトランザクションを取得")
        
        # 次のページがあるかチェック
        if 'pagination' in data and 'nextToken' in data['pagination']:
            next_token = data['pagination']['nextToken']
            time.sleep(2)  # レート制限対策
        else:
            break
    
    print(f"  合計 {len(all_transactions)}件のトランザクションを取得しました")
    return all_transactions


def save_to_gcs(transactions, jst_date_str, bucket_name, prefix):
    """
    トランザクションデータをGCSに保存します。
    
    Args:
        transactions: トランザクションのリスト
        jst_date_str: 日本時間の日付 (YYYY-MM-DD)
        bucket_name: GCSバケット名
        prefix: GCSプレフィックス
    """
    if not transactions:
        print("  保存するデータがありません")
        return
    
    # NDJSON形式に変換
    ndjson_lines = [json.dumps(txn, ensure_ascii=False) for txn in transactions]
    ndjson_content = "\n".join(ndjson_lines)
    
    # ファイル名生成 (YYYYMMDD.json) - 日本時間の日付を使用
    filename = f"{jst_date_str.replace('-', '')}.json"
    blob_name = f"{prefix}{filename}"
    
    # GCSにアップロード
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        blob.upload_from_string(
            ndjson_content,
            content_type='application/x-ndjson'
        )
        
        print(f"  ✓ GCSに保存完了: gs://{bucket_name}/{blob_name}")
        print(f"    {len(transactions)}件のトランザクション")
    
    except Exception as e:
        print(f"  ✗ GCS保存エラー: {e}")
        raise


def run():
    """
    Transactions APIエンドポイントのメイン処理
    """
    print("\n" + "=" * 60)
    print("Transactions API - データ取得開始")
    print("=" * 60)
    
    try:
        # 日付範囲を計算
        posted_after, posted_before, jst_date_str = get_posted_date_range()
        
        print(f"\n対象期間(UTC): {posted_after} ~ {posted_before}")
        print(f"対象日(JST): {jst_date_str}")
        
        # アクセストークン取得
        print("\nアクセストークンを取得中...")
        access_token = get_access_token()
        
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': access_token
        }
        
        # トランザクション取得
        print("\nトランザクションを取得中...")
        transactions = fetch_transactions(posted_after, posted_before, headers)
        
        if not transactions:
            print("\n✓ トランザクションが見つかりませんでした")
            return
        
        # GCSに保存
        print("\nGCSに保存中...")
        save_to_gcs(transactions, jst_date_str, GCS_BUCKET, GCS_PREFIX)
        
        print("\n" + "=" * 60)
        print("✓ Transactions API - 処理完了")
        print("=" * 60)
    
    except Exception as e:
        print(f"\n✗ エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    run()
