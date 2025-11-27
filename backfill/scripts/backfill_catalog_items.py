"""
Catalog Items - Snapshot Backfill

このスクリプトは、現在の全ASINのカタログ情報を取得し、バックフィル用ディレクトリに保存します。
ASINリストはFBA Inventory APIから取得します。
Catalog Items APIは過去の情報を取得できないため、実行時点のスナップショットを保存します。
保存先: backfill/data/catalog-items/
"""

import json
import sys
import time
from pathlib import Path
from datetime import datetime

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.http_retry import request_with_retry
from backfill.scripts.auth import get_access_token
from backfill.scripts.backfill_fba_inventory import _get_all_inventory_summaries


# ===================================================================
# 設定
# ===================================================================
MARKETPLACE_ID = "A1VC38T7YXB528"  # 日本
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
DATA_DIR = Path(__file__).parent.parent / "data" / "catalog-items"

# カタログ情報に含めるデータ
INCLUDED_DATA = [
    "summaries",
    "attributes",
    "classifications",
    "dimensions",
    "identifiers",
    "images",
    "productTypes",
    "relationships",
    "salesRanks"
]


def get_asin_list(access_token):
    """FBA在庫からASINリストを取得します。"""
    try:
        summaries = _get_all_inventory_summaries(access_token)
        
        # ASINを抽出（重複を除く）
        asin_set = set()
        for summary in summaries:
            asin = summary.get("asin")
            if asin:
                asin_set.add(asin)
        
        return sorted(list(asin_set))
        
    except Exception as e:
        print(f"  -> Error: ASIN一覧の取得に失敗しました: {e}")
        raise


def _fetch_catalog_item(access_token, asin):
    """指定されたASINのカタログ情報を取得します。"""
    url = f"{SP_API_ENDPOINT}/catalog/2022-04-01/items/{asin}"
    
    headers = {
        "x-amz-access-token": access_token,
        "Accept": "application/json"
    }
    
    params = {
        "marketplaceIds": MARKETPLACE_ID,
        "includedData": ",".join(INCLUDED_DATA)
    }
    
    try:
        # レート制限対策: 0.5秒待機 (6 req/s limit)
        time.sleep(0.5)
        
        response = request_with_retry("GET", url, headers=headers, params=params)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            print(f"    -> Warn: ASIN {asin} が見つかりません（404）")
            return None
        else:
            error_msg = f"Catalog API Error: {response.status_code} - {response.text}"
            print(f"    -> Error: {error_msg}")
            return None
            
    except Exception as e:
        print(f"    -> Error: ASIN {asin} の取得に失敗: {e}")
        return None


def backfill():
    """Catalog Itemsデータのバックフィル（スナップショット取得）を実行します。"""
    print("\\n=== Catalog Items バックフィル（スナップショット）開始 ===")
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    try:
        # アクセストークン取得
        access_token = get_access_token()
        
        # ASIN一覧を取得
        print("\\n[1/2] ASIN一覧を取得中...")
        asin_list = get_asin_list(access_token)
        
        if not asin_list:
            print("  -> Warn: ASIN一覧が空です")
            return
        
        print(f"  -> 取得対象ASIN数: {len(asin_list)}")
        
        # 各ASINのカタログ情報を取得
        print(f"\\n[2/2] カタログ情報を取得中（全{len(asin_list)}件）...")
        
        current_date = datetime.now().strftime("%Y%m%d")
        success_count = 0
        error_count = 0
        
        for i, asin in enumerate(asin_list, 1):
            filename = f"catalog_item_{asin}_{current_date}.json"
            filepath = DATA_DIR / filename
            
            if filepath.exists():
                print(f"  [{i}/{len(asin_list)}] {asin} (スキップ: 既存)")
                continue
            
            print(f"  [{i}/{len(asin_list)}] {asin}")
            
            # カタログ情報を取得
            catalog_data = _fetch_catalog_item(access_token, asin)
            
            if catalog_data:
                # メタデータを追加
                output_data = {
                    "fetchedAt": datetime.now().isoformat(),
                    "marketplaceId": MARKETPLACE_ID,
                    "asin": asin,
                    "catalogData": catalog_data
                }
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, ensure_ascii=False, indent=2)
                
                success_count += 1
            else:
                error_count += 1
        
        print(f"\\nCatalog Items完了: 成功 {success_count}件, エラー {error_count}件")
        
    except Exception as e:
        print(f"\\n  -> Error: Catalog Items処理中にエラーが発生しました: {e}")
        raise

    print("\\n=== Catalog Items バックフィル完了 ===")


if __name__ == "__main__":
    print("Catalog Items - Snapshot Backfill")
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
