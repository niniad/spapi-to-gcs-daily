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
    
    Returns:
        tuple: (posted_after, posted_before) のISO 8601形式文字列
    """
    utc_now = datetime.now(timezone.utc)
    # 昨日の00:00:00から今日の00:00:00まで
    posted_before = utc_now.replace(hour=0, minute=0, second=0, microsecond=0)
    posted_after = posted_before - timedelta(days=1)
    
    return (
        posted_after.strftime('%Y-%m-%dT%H:%M:%SZ'),
        posted_before.strftime('%Y-%m-%dT%H:%M:%SZ')
    )


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


def save_to_gcs(transactions, posted_date, bucket_name, prefix):
    """
    トランザクションデータをGCSに保存します。
    
    Args:
        transactions: トランザクションのリスト
        posted_date: 投稿日 (YYYY-MM-DD)
        bucket_name: GCSバケット名
        prefix: GCSプレフィックス
    """
    if not transactions:
        print("  保存するデータがありません")
        return
    
    # NDJSON形式に変換
    ndjson_lines = [json.dumps(txn, ensure_ascii=False) for txn in transactions]
    ndjson_content = "\n".join(ndjson_lines)
    
    # ファイル名生成 (YYYYMMDD.json)
    filename = f"{posted_date.replace('-', '')}.json"
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
        posted_after, posted_before = get_posted_date_range()
        posted_date = datetime.fromisoformat(posted_after.replace('Z', '+00:00')).strftime('%Y-%m-%d')
        
        print(f"\n対象期間: {posted_after} ~ {posted_before}")
        print(f"対象日: {posted_date}")
        
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
        save_to_gcs(transactions, posted_date, GCS_BUCKET, GCS_PREFIX)
        
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
