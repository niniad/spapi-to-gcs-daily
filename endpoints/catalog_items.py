"""
Catalog Items Module

このモジュールは、SP-APIのCatalog Items APIを使用して商品カタログ情報を取得し、GCSに保存します。
ASINリストはFBA Inventory APIから取得します。
"""

import json
import time
from datetime import datetime
from google.cloud import storage
from utils.sp_api_auth import get_access_token
from utils.http_retry import request_with_retry



# ===================================================================
# 設定
# ===================================================================
MARKETPLACE_ID = "A1VC38T7YXB528"  # 日本
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
GCS_BUCKET_NAME = "sp-api-bucket"
GCS_FILE_PREFIX = "catalog-items/"

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


def _upload_to_gcs(bucket_name, blob_name, content):
    """
    GCSにファイルをアップロードします。
    
    Args:
        bucket_name: GCSバケット名
        blob_name: 保存するファイル名
        content: ファイルの内容（JSON文字列）
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content, content_type='application/json')
        print(f"    -> GCS保存成功: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f"    -> Error: GCSアップロード失敗: {e}")


def _fetch_catalog_item(access_token, asin):
    """
    指定されたASINのカタログ情報を取得します。
    
    Args:
        access_token: SP-APIアクセストークン
        asin: 商品ASIN
        
    Returns:
        dict: カタログ情報、またはNone（エラー時）
    """
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


def run():
    """
    全ASINのカタログ情報を取得してGCSに保存します。
    """
    print("\n" + "=" * 60)
    print("Catalog Items - 処理開始")
    print("=" * 60)
    
    try:
        # アクセストークン取得
        access_token = get_access_token()
        
        # ASIN一覧を取得 (FBA InventoryのGCS保存結果から)
        print("\n[1/3] ASIN一覧を取得中 (from GCS)...")
        # get_asin_list() は廃止 (APIコールの重複を防ぐため)
        # asin_list = get_asin_list() 
        
        asin_list = []
        try:
            current_date = datetime.now().strftime("%Y%m%d")
            inventory_filename = f"fba-inventory/{current_date}.json"
            
            storage_client = storage.Client()
            bucket = storage_client.bucket(GCS_BUCKET_NAME)
            blob = bucket.blob(inventory_filename)
            
            if blob.exists():
                content = blob.download_as_text()
                # NDJSONをパースしてASINを抽出
                for line in content.splitlines():
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    asin = data.get("inventorySummary", {}).get("asin")
                    if asin:
                        asin_list.append(asin)
                
                # ユニーク化
                asin_list = sorted(list(set(asin_list)))
                print(f"  -> GCS Inventory Found: {inventory_filename}")
            else:
                print(f"  -> Warn: FBA在庫ファイルが見つかりません: {inventory_filename}")
                print("  -> FBA Inventory APIが先に実行されているか確認してください。")
                return

        except Exception as e:
            print(f"  -> Error: GCSからのASIN一覧取得に失敗: {e}")
            return
        
        if not asin_list:
            print("  -> Warn: ASIN一覧が空です")
            return
        
        print(f"  -> 取得対象ASIN数: {len(asin_list)}")
        
        # 各ASINのカタログ情報を取得
        print(f"\n[2/3] カタログ情報を取得中（全{len(asin_list)}件）...")
        
        current_date = datetime.now().strftime("%Y%m%d")
        all_catalog_data = []
        success_count = 0
        error_count = 0
        
        for i, asin in enumerate(asin_list, 1):
            print(f"  [{i}/{len(asin_list)}] ASIN: {asin}")
            
            # カタログ情報を取得
            catalog_data = _fetch_catalog_item(access_token, asin)
            
            if catalog_data:
                # メタデータを追加
                item_data = {
                    "fetchedAt": datetime.now().isoformat(),
                    "marketplaceId": MARKETPLACE_ID,
                    "asin": asin,
                    "catalogData": catalog_data
                }
                all_catalog_data.append(item_data)
                success_count += 1
            else:
                error_count += 1
        
        # まとめてGCSに保存 (NDJSON形式)
        if all_catalog_data:
            print(f"\n[3/3] GCSに保存中 ({len(all_catalog_data)}件)...")
            
            # ファイル名を生成 (YYYYMMDD.json)
            blob_name = f"{GCS_FILE_PREFIX}{current_date}.json"
            
            # NDJSON形式で保存
            ndjson_lines = [json.dumps(item, ensure_ascii=False) for item in all_catalog_data]
            ndjson_content = "\n".join(ndjson_lines)
            
            _upload_to_gcs(GCS_BUCKET_NAME, blob_name, ndjson_content)
        else:
            print("\n  -> Warn: 保存するデータがありません")
                
        
        print("\n" + "=" * 60)
        print("Catalog Items - 処理完了")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n  -> Error: Catalog Items処理中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    run()
