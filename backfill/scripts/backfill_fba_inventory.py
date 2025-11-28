"""
FBA Inventory - Snapshot Backfill

このスクリプトは、現在のFBA在庫情報を取得し、バックフィル用ディレクトリに保存します。
FBA Inventory APIは過去の在庫情報を取得できないため、実行時点のスナップショットを保存します。
保存先: backfill/data/fba-inventory/
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.http_retry import request_with_retry
from backfill.scripts.auth import get_access_token


# ===================================================================
# 設定
# ===================================================================
MARKETPLACE_ID = "A1VC38T7YXB528"  # 日本
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
DATA_DIR = Path(__file__).parent.parent / "data" / "fba-inventory"


def _fetch_inventory_summaries(access_token, next_token=None):
    """FBA Inventory APIから在庫サマリーを取得します。"""
    url = f"{SP_API_ENDPOINT}/fba/inventory/v1/summaries"
    
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    params = {
        "marketplaceIds": MARKETPLACE_ID,
        "granularityType": "Marketplace",
        "granularityId": MARKETPLACE_ID
    }
    
    if next_token:
        params["nextToken"] = next_token
    
    response = request_with_retry("GET", url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        error_msg = f"FBA Inventory API Error: {response.status_code} - {response.text}"
        print(f"  -> Error: {error_msg}")
        raise Exception(error_msg)


def _get_all_inventory_summaries(access_token):
    """全ての在庫サマリーを取得します（ページネーション対応）。"""
    all_summaries = []
    next_token = None
    page = 1
    
    print("  -> FBA在庫情報を取得中...")
    
    while True:
        print(f"    ページ {page} を取得中...")
        response_data = _fetch_inventory_summaries(access_token, next_token)
        
        # Handle payload wrapper if present
        if "payload" in response_data:
            summaries = response_data["payload"].get("inventorySummaries", [])
        else:
            summaries = response_data.get("inventorySummaries", [])
            
        all_summaries.extend(summaries)
        
        print(f"    {len(summaries)} 件の在庫情報を取得")
        
        # 次のページがあるかチェック
        pagination = response_data.get("pagination", {})
        next_token = pagination.get("nextToken")
        
        if not next_token:
            break
            
        page += 1
    
    print(f"  -> 合計 {len(all_summaries)} 件の在庫情報を取得完了")
    return all_summaries


def backfill():
    """FBA Inventoryデータのバックフィル（スナップショット取得）を実行します。"""
    print("\\n=== FBA Inventory バックフィル（スナップショット）開始 ===")
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    try:
        # アクセストークン取得
        access_token = get_access_token()
        
        # 全在庫情報を取得
        summaries = _get_all_inventory_summaries(access_token)
        
        if not summaries:
            print("  -> Warn: 在庫情報が見つかりませんでした")
            return
        
        # 現在日時でファイル名を生成 (YYYYMMDD.json)
        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"{current_date}.json"
        filepath = DATA_DIR / filename
        
        # NDJSON形式で保存
        with open(filepath, 'w', encoding='utf-8') as f:
            for summary in summaries:
                # メタデータを追加
                item_data = {
                    "fetchedAt": datetime.now().isoformat(),
                    "marketplaceId": MARKETPLACE_ID,
                    "inventorySummary": summary
                }
                f.write(json.dumps(item_data, ensure_ascii=False) + '\n')
            
        print(f"  ✓ 保存完了: {filepath}")
        
        # ASIN一覧を表示
        asin_list = [s.get("asin") for s in summaries if s.get("asin")]
        unique_asins = sorted(set(asin_list))
        print(f"  -> ユニークASIN数: {len(unique_asins)}")
        
    except Exception as e:
        print(f"\\n  -> Error: FBA Inventory処理中にエラーが発生しました: {e}")
        raise

    print("\\n=== FBA Inventory バックフィル完了 ===")


if __name__ == "__main__":
    print("FBA Inventory - Snapshot Backfill")
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
