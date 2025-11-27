"""
Transactions API - Historical Data Backfill

このスクリプトは、過去18ヶ月分のトランザクションデータを日次で取得します。
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
        datetime: 各日の日付
    """
    utc_now = datetime.now(timezone.utc)
    current_date = utc_now - timedelta(days=1)  # 昨日から開始
    cutoff_date = utc_now - timedelta(days=30 * BACKFILL_MONTHS)
    
    while current_date >= cutoff_date:
        yield current_date
        current_date -= timedelta(days=1)


def fetch_transactions(target_date, headers):
    """
    指定日のトランザクションを取得します。
    
    Args:
        target_date: 対象日
        headers: HTTPヘッダー
        
    Returns:
        tuple: (transactions, is_rate_limited)
            transactions (list): トランザクションのリスト
            is_rate_limited (bool): レート制限にかかったかどうか
    """
    # 日付範囲: 対象日の00:00:00から翌日の00:00:00まで
    posted_after = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    posted_before = posted_after + timedelta(days=1)
    
    posted_after_str = posted_after.strftime('%Y-%m-%dT%H:%M:%SZ')
    posted_before_str = posted_before.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    all_transactions = []
    next_token = None
    
    while True:
        params = {
            'postedAfter': posted_after_str,
            'postedBefore': posted_before_str,
            'marketplaceId': MARKETPLACE_ID
        }
        
        if next_token:
            params['nextToken'] = next_token
            
        try:
            response = request_with_retry(
                'GET',
                f"{SP_API_ENDPOINT}/finances/2024-06-19/transactions",
                headers=headers,
                params=params,
                max_retries=5,
                retry_delay=60
            )
            
            data = response.json()
            
            if 'payload' in data and 'transactions' in data['payload']:
                transactions = data['payload']['transactions']
                all_transactions.extend(transactions)
            
            if 'pagination' in data and 'nextToken' in data['pagination']:
                next_token = data['pagination']['nextToken']
                time.sleep(1)
            else:
                break
                
        except Exception as e:
            if "429" in str(e):
                return None, True
            print(f"      エラー: {e}")
            return None, False
            
    return all_transactions, False


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
    max_consecutive_errors = 5
    
    for target_date in get_all_date_ranges():
        total_count += 1
        filename = f"{target_date.strftime('%Y%m%d')}.json"
        filepath = DATA_DIR / filename
        
        if filepath.exists():
            if total_count % 50 == 0:
                print(f"  進捗: {total_count}日処理済み (成功: {success_count}, スキップ: {skip_count})")
            skip_count += 1
            continue
        
        print(f"  [{total_count}] {filename}")
        transactions, is_rate_limited = fetch_transactions(target_date, headers)
        
        if transactions is not None:
            # NDJSON形式で保存
            if transactions:
                with open(filepath, 'w', encoding='utf-8') as f:
                    for txn in transactions:
                        f.write(json.dumps(txn, ensure_ascii=False) + '\\n')
                print(f"    ✓ 保存完了 ({len(transactions)}件)")
            else:
                # 空ファイルを作成して処理済みとする
                filepath.touch()
                print(f"    データなし")
                
            success_count += 1
            consecutive_errors = 0
        else:
            print(f"    取得失敗")
            skip_count += 1
            consecutive_errors += 1
            
            if consecutive_errors >= max_consecutive_errors:
                print(f"\\n  連続エラーが{max_consecutive_errors}回発生しました。60秒待機します...")
                time.sleep(60)
                consecutive_errors = 0
            
            if is_rate_limited:
                print(f"  レート制限回避のため120秒待機します...")
                time.sleep(120)
                continue
        
        time.sleep(2)  # 待機
        
        if total_count % 50 == 0:
            print(f"\\n  進捗: {total_count}日処理済み (成功: {success_count}, スキップ: {skip_count})\\n")
    
    print(f"\\nTransactions完了: 成功 {success_count}件, スキップ {skip_count}件")


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
