"""
Orders API Backfill Script

このスクリプトは、SP-APIのOrders API (getOrders) を使用して過去の注文データを取得し、JSONL形式で保存します。
取得したデータは backfill/data/orders/ に保存されます。
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
DATA_DIR = Path(__file__).parent.parent / "data" / "orders"
ORDERS_API_PATH = "/orders/v0/orders"

# バックフィル期間（過去2年）
BACKFILL_YEARS = 2


def fetch_orders_for_date(date_str, access_token):
    """
    指定された日付の注文データを取得します。
    """
    orders = []
    next_token = None
    
    # 期間設定: 指定日の 00:00:00 〜 23:59:59 (UTC)
    start_ts = f"{date_str}T00:00:00Z"
    end_ts = f"{date_str}T23:59:59Z"
    
    headers = {
        'x-amz-access-token': access_token,
        'Content-Type': 'application/json'
    }

    while True:
        params = {
            "MarketplaceIds": [MARKETPLACE_ID],
            "LastUpdatedAfter": start_ts,
            "LastUpdatedBefore": end_ts,
        }
        if next_token:
            params = {"NextToken": next_token}
        
        try:
            response = request_with_retry(
                'GET',
                f"{SP_API_ENDPOINT}{ORDERS_API_PATH}",
                headers=headers,
                params=params,
                max_retries=3,
                retry_delay=2
            )
            
            data = response.json()
            payload = data.get("payload", {})
            fetched_orders = payload.get("Orders", [])
            orders.extend(fetched_orders)
            
            next_token = payload.get("NextToken")
            if not next_token:
                break
            
            time.sleep(1) # レートリミット考慮
            
        except Exception as e:
            print(f"    -> Error: APIリクエスト失敗: {e}")
            raise e
            
    return orders


def backfill_orders():
    """Orders APIのバックフィルを実行します。"""
    print("\n=== Orders API バックフィル開始 ===")
    
    utc_now = datetime.now(timezone.utc)
    start_date = utc_now - timedelta(days=1) # 昨日から開始
    cutoff_date = utc_now - timedelta(days=365 * BACKFILL_YEARS)
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    current_date = start_date
    access_token = get_access_token()
    
    success_count = 0
    skip_count = 0
    
    while current_date >= cutoff_date:
        date_str = current_date.strftime('%Y%m%d')
        date_iso = current_date.strftime('%Y-%m-%d')
        
        filename = f"{date_str}.jsonl"
        filepath = DATA_DIR / filename
        
        if filepath.exists():
            print(f"  [SKIP] {filename} (既存)")
            skip_count += 1
            current_date -= timedelta(days=1)
            continue
            
        print(f"  [取得中] {filename} ({date_iso})")
        
        try:
            orders = fetch_orders_for_date(date_iso, access_token)
            
            if orders:
                # JSONL形式で保存
                with open(filepath, 'w', encoding='utf-8') as f:
                    for order in orders:
                        f.write(json.dumps(order, ensure_ascii=False) + "\n")
                print(f"    ✓ {len(orders)}件 保存完了")
                success_count += 1
            else:
                print(f"    - データなし")
                # データなしでも空ファイルを作るかどうかは要件次第だが、
                # 再実行時にスキップさせるために空ファイルを作っておくのが一般的
                with open(filepath, 'w', encoding='utf-8') as f:
                    pass
                success_count += 1
                
        except Exception as e:
            print(f"    ! Error: {e}")
            # エラー時は停止せず次へ進むか、トークン更新などが必要か
            # ここでは単純にウェイトを入れてリトライ等はしない（上位関数でハンドリングしていないため）
            pass
        
        current_date -= timedelta(days=1)
        time.sleep(1) # レート制限対策

    print(f"\nOrders API バックフィル完了: 成功 {success_count}件, スキップ {skip_count}件")


if __name__ == "__main__":
    try:
        backfill_orders()
    except KeyboardInterrupt:
        print("\n中断されました")
    except Exception as e:
        print(f"\nエラー: {e}")
        import traceback
        traceback.print_exc()
