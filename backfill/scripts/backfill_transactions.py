"""
Transactions API - Historical Data Backfill

このスクリプトは、過去18ヶ月分のTransactionsデータを日次で取得します。
取得したデータは backfill/data/transactions/ に保存されます。
"""

import json
import time
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.http_retry import request_with_retry
from backfill.scripts.auth import get_access_token


# ===================================================================
# 設定
# ===================================================================
MARKETPLACE_ID = "A1VC38T7YXB528"  # 日本
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
DATA_DIR = Path(__file__).parent.parent / "data" / "transactions"

# バックフィル期間（過去18ヶ月）
BACKFILL_MONTHS = 18


def get_all_date_ranges():
    """
    現在日から過去18ヶ月分の日付を生成します。
    
    Yields:
        tuple: (posted_after, posted_before, date_str)
    """
    utc_now = datetime.now(timezone.utc)
    current_date = utc_now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    cutoff_date = utc_now - timedelta(days=30 * BACKFILL_MONTHS)
    
    while current_date >= cutoff_date:
        posted_after = current_date
        posted_before = current_date + timedelta(days=1)
        
        yield (
            posted_after.strftime('%Y-%m-%dT%H:%M:%SZ'),
            posted_before.strftime('%Y-%m-%dT%H:%M:%SZ'),
            current_date.strftime('%Y-%m-%d')
        )
        
        current_date -= timedelta(days=1)


def fetch_transactions(posted_after, posted_before, headers):
    """
    指定期間のトランザクションを取得します。
    
    Args:
        posted_after: 開始日時 (ISO 8601)
        posted_before: 終了日時 (ISO 8601)
        headers: HTTPヘッダー
        
    Returns:
        list: トランザクションのリスト、またはNone
    """
    all_transactions = []
    next_token = None
    page_count = 0
    
    try:
        while True:
            page_count += 1
            
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
            
            # 次のページがあるかチェック
            if 'pagination' in data and 'nextToken' in data['pagination']:
                next_token = data['pagination']['nextToken']
                time.sleep(1)  # ページネーション間の待機
            else:
                break
        
        return all_transactions if all_transactions else None
    
    except Exception as e:
        print(f"      エラー: {e}")
        return None


def backfill():
    """Transactionsデータのバックフィルを実行します。"""
    print("\\n=== Transactions データのバックフィル開始 ===")
    
    access_token = get_access_token()
    headers = {
        'Content-Type': 'application/json',
        'x-amz-access-token': access_token
    }
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    success_count = 0
    skip_count = 0
    total_count = 0
    consecutive_errors = 0
    max_consecutive_errors = 10
    
    for posted_after, posted_before, date_str in get_all_date_ranges():
        total_count += 1
        filename = f"{date_str.replace('-', '')}.json"
        filepath = DATA_DIR / filename
        
        if filepath.exists():
            if total_count % 50 == 0:
                print(f"  進捗: {total_count}日処理済み (成功: {success_count}, スキップ: {skip_count})")
            skip_count += 1
            continue
        
        print(f"  [{total_count}] {filename}")
        transactions = fetch_transactions(posted_after, posted_before, headers)
        
        if transactions:
            # NDJSON形式で保存
            ndjson_lines = [json.dumps(txn, ensure_ascii=False) for txn in transactions]
            ndjson_content = "\\n".join(ndjson_lines)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(ndjson_content)
            
            print(f"    ✓ 保存完了 ({len(transactions)}件)")
            success_count += 1
            consecutive_errors = 0
        else:
            print(f"    データなし")
            skip_count += 1
            consecutive_errors += 1
            
            # 連続エラー時は待機時間を増やす
            if consecutive_errors >= max_consecutive_errors:
                print(f"\\n  連続エラーが{max_consecutive_errors}回発生しました。60秒待機します...")
                time.sleep(60)
                consecutive_errors = 0
            elif consecutive_errors >= 5:
                wait_time = min(30, consecutive_errors * 3)
                print(f"  {wait_time}秒待機します...")
                time.sleep(wait_time)
                continue
        
        time.sleep(3)  # レート制限対策
        
        # 進捗表示
        if total_count % 50 == 0:
            print(f"\\n  進捗: {total_count}日処理済み (成功: {success_count}, スキップ: {skip_count})\\n")
    
    print(f"\\nTransactions完了: 成功 {success_count}件, スキップ {skip_count}件, 合計 {total_count}件")


if __name__ == "__main__":
    print("Transactions - Historical Data Backfill")
    print("=" * 60)
    
    try:
        backfill()
        
        print("\\n" + "=" * 60)
        print("バックフィル完了")
        
    except KeyboardInterrupt:
        print("\\n\\n中断されました")
    except Exception as e:
        print(f"\\nエラー: {e}")
        import traceback
        traceback.print_exc()
