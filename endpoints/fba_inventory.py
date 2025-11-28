"""
FBA Inventory Module

このモジュールは、SP-APIのFBA Inventory APIを使用して在庫情報を取得し、GCSに保存します。
取得した在庫情報から、ASINリストを抽出して他のエンドポイントで使用できるようにします。
"""

import json
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
GCS_FILE_PREFIX = "fba-inventory/"


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
        print(f"  -> GCSへの保存成功: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f"  -> Error: GCSへのアップロードに失敗しました: {e}")


def _fetch_inventory_summaries(access_token, next_token=None):
    """
    FBA Inventory APIから在庫サマリーを取得します。
    
    Args:
        access_token: SP-APIアクセストークン
        next_token: ページネーション用のトークン（オプション）
        
    Returns:
        dict: APIレスポンス
    """
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
    """
    全ての在庫サマリーを取得します（ページネーション対応）。
    
    Args:
        access_token: SP-APIアクセストークン
        
    Returns:
        list: 全在庫サマリーのリスト
    """
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


def get_asin_list():
    """
    FBA在庫からASINリストを取得します。
    
    Returns:
        list: ASINのリスト
    """
    try:
        access_token = get_access_token()
        summaries = _get_all_inventory_summaries(access_token)
        
        # ASINを抽出（重複を除く）
        asin_set = set()
        for summary in summaries:
            asin = summary.get("asin")
            if asin:
                asin_set.add(asin)
        
        asin_list = sorted(list(asin_set))
        print(f"  -> 抽出されたASIN数: {len(asin_list)}")
        
        return asin_list
        
    except Exception as e:
        print(f"  -> Error: ASIN一覧の取得に失敗しました: {e}")
        raise

def run():
    """
    FBA在庫情報を取得してGCSに保存します。
    """
    print("\n" + "=" * 60)
    print("FBA Inventory - 処理開始")
    print("=" * 60)
    
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
        filename = f"{GCS_FILE_PREFIX}{current_date}.json"
        
        # NDJSON形式で保存
        ndjson_lines = []
        for summary in summaries:
            # メタデータを追加
            item_data = {
                "fetchedAt": datetime.now().isoformat(),
                "marketplaceId": MARKETPLACE_ID,
                "inventorySummary": summary
            }
            ndjson_lines.append(json.dumps(item_data, ensure_ascii=False))
            
        ndjson_content = "\n".join(ndjson_lines)
        
        # GCSにアップロード
        _upload_to_gcs(GCS_BUCKET_NAME, filename, ndjson_content)
        
        # ASIN一覧を表示
        asin_list = [s.get("asin") for s in summaries if s.get("asin")]
        unique_asins = sorted(set(asin_list))
        print(f"\n  -> ユニークASIN数: {len(unique_asins)}")
        print(f"  -> ASIN一覧: {', '.join(unique_asins[:10])}{'...' if len(unique_asins) > 10 else ''}")
        
        print("\n" + "=" * 60)
        print("FBA Inventory - 処理完了")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n  -> Error: FBA Inventory処理中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    run()
