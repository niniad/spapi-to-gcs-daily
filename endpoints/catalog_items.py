"""
Catalog Items Module

このモジュールは、SP-APIのCatalog Items APIを使用して商品カタログ情報を取得し、GCSに保存します。
ASINリストはFBA Inventory APIから取得します。
"""

import logging
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
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content, content_type='application/json')
        logging.info(f"GCS保存成功: gs://{bucket_name}/{blob_name}")
    except Exception:
        logging.error(f"GCSアップロード失敗: gs://{bucket_name}/{blob_name}", exc_info=True)


def _fetch_catalog_item(access_token, asin):
    """
    指定されたASINのカタログ情報を取得します。
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
            logging.warning(f"ASIN {asin} が見つかりません（404）")
            return None
        else:
            logging.error(f"Catalog API Error for ASIN {asin}: {response.status_code} - {response.text}")
            return None
            
    except Exception:
        logging.error(f"ASIN {asin} の取得に失敗", exc_info=True)
        return None


def run():
    """
    全ASINのカタログ情報を取得してGCSに保存します。
    """
    logging.info("=== Catalog Items - 処理開始 ===")
    
    try:
        access_token = get_access_token()
        
        logging.info("[1/3] GCSからASIN一覧を取得中...")
        
        asin_list = []
        try:
            current_date = datetime.now().strftime("%Y%m%d")
            inventory_filename = f"fba-inventory/{current_date}.json"
            
            storage_client = storage.Client()
            bucket = storage_client.bucket(GCS_BUCKET_NAME)
            blob = bucket.blob(inventory_filename)
            
            if blob.exists():
                content = blob.download_as_text()
                for line in content.splitlines():
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    asin = data.get("inventorySummary", {}).get("asin")
                    if asin:
                        asin_list.append(asin)
                
                asin_list = sorted(list(set(asin_list)))
                logging.info(f"GCS Inventory Found: {inventory_filename}")
            else:
                logging.warning(f"FBA在庫ファイルが見つかりません: {inventory_filename}")
                logging.warning("FBA Inventory APIが先に実行されているか確認してください。")
                return

        except Exception:
            logging.error("GCSからのASIN一覧取得に失敗", exc_info=True)
            return
        
        if not asin_list:
            logging.warning("ASIN一覧が空です")
            return
        
        logging.info(f"取得対象ASIN数: {len(asin_list)}")
        
        logging.info(f"[2/3] カタログ情報を取得中（全{len(asin_list)}件）...")
        
        all_catalog_data = []
        success_count = 0
        
        for i, asin in enumerate(asin_list, 1):
            logging.info(f"[{i}/{len(asin_list)}] ASIN: {asin}")
            
            catalog_data = _fetch_catalog_item(access_token, asin)
            
            if catalog_data:
                item_data = {
                    "fetchedAt": datetime.now().isoformat(),
                    "marketplaceId": MARKETPLACE_ID,
                    "asin": asin,
                    "catalogData": catalog_data
                }
                all_catalog_data.append(item_data)
                success_count += 1
            
            # Rate limit
            time.sleep(1.5)
        
        if all_catalog_data:
            logging.info(f"[3/3] GCSに保存中 ({len(all_catalog_data)}件)...")
            
            blob_name = f"{GCS_FILE_PREFIX}{datetime.now().strftime('%Y%m%d')}.json"
            ndjson_lines = [json.dumps(item, ensure_ascii=False) for item in all_catalog_data]
            ndjson_content = "\n".join(ndjson_lines)
            
            _upload_to_gcs(GCS_BUCKET_NAME, blob_name, ndjson_content)
        else:
            logging.warning("保存するデータがありません")
                
        logging.info(f"処理サマリー: 成功={success_count}, 失敗/スキップ={len(asin_list) - success_count}")
        logging.info("=== Catalog Items - 処理完了 ===")
        
    except Exception:
        logging.critical("Catalog Items処理中に致命的なエラーが発生しました", exc_info=True)
        raise
