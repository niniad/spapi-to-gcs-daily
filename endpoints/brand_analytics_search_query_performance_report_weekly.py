"""
Brand Analytics Search Query Performance Report (WEEKLY) Module

このモジュールは、SP-APIのBrand Analytics Search Query Performance Report (WEEK)を取得し、GCSに保存します。
- 取得期間: 直近の完全な1週間（日曜日〜土曜日）
- 頻度: 毎日実行（期間が重複する場合は上書き）
- ASINリスト: FBA Inventory APIから自動取得
"""

import logging
import json
import time
import gzip
import io
import requests
from datetime import datetime, timedelta, timezone
from google.cloud import storage
from utils.sp_api_auth import get_access_token
from utils.http_retry import request_with_retry
from endpoints.fba_inventory import get_asin_list


# ===================================================================
# 設定
# ===================================================================
MARKETPLACE_ID = "A1VC38T7YXB528"  # 日本
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
GCS_BUCKET_NAME = "sp-api-bucket"
GCS_FILE_PREFIX = "brand-analytics-search-query-performance-report/"


def _upload_to_gcs(bucket_name, blob_name, content):
    """
    GCSにファイルをアップロードします。
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content, content_type='application/json')
        logging.info(f"GCSへの保存成功: gs://{bucket_name}/{blob_name}")
    except Exception:
        logging.error(f"GCSへのアップロードに失敗しました: gs://{bucket_name}/{blob_name}", exc_info=True)


def _get_last_complete_week_range():
    """
    直近の完全な1週間（日曜日〜土曜日）の開始日と終了日を計算します。
    """
    utc_now = datetime.now(timezone.utc)
    weekday = utc_now.weekday()  # Monday=0, ..., Sunday=6
    days_since_saturday = (weekday - 5) % 7
    # if today is Sat(5), days_since_saturday = 0. We need last week's Sat, so need to go back 7 days
    # if today is Sun(6), days_since_saturday = 1. The last Sat was 1 day ago.
    # if today is Fri(4), days_since_saturday = -1 % 7 = 6. The last Sat was 6 days ago.
    # To make it simple and safe, let's target the Saturday before last.
    days_to_last_saturday = (weekday - 5 + 7) % 7
    end_date_of_report_week = utc_now - timedelta(days=days_to_last_saturday + 1) #+1 to get previous week
    start_date_of_report_week = end_date_of_report_week - timedelta(days=6)
    return start_date_of_report_week, end_date_of_report_week


def run():
    """
    Brand Analytics Search Query Performance Report (WEEK) の取得とGCS保存を実行します。
    """
    logging.info("=== Brand Analytics Search Query Performance Report (WEEK) 処理開始 ===")
    
    try:
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': access_token
        }

        logging.info("FBA InventoryからASIN一覧を取得中...")
        asin_list = get_asin_list()
        logging.info(f"取得ASIN数: {len(asin_list)}")

        period = "WEEK"
        gcs_folder = "WEEK"
        
        logging.info(f"--- {period} レポート処理開始 ---")
        
        start_date, end_date = _get_last_complete_week_range()
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        logging.info(f"データ取得期間: {start_date_str} から {end_date_str}")
        
        all_ndjson_lines = []
        chunk_size = 10
        num_chunks = (len(asin_list) + chunk_size - 1) // chunk_size
        
        logging.info(f"ASIN数: {len(asin_list)} ({chunk_size}件ずつ {num_chunks}回に分割して取得)")
        
        for i in range(0, len(asin_list), chunk_size):
            chunk = asin_list[i:i + chunk_size]
            chunk_num = i // chunk_size + 1
            asin_str = " ".join(chunk)
            
            payload_dict = {
                "marketplaceIds": [MARKETPLACE_ID],
                "reportType": "GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT",
                "dataStartTime": f"{start_date_str}T00:00:00.000Z",
                "dataEndTime": f"{end_date_str}T00:00:00.000Z",
                "reportOptions": {
                    "reportPeriod": period,
                    "asin": asin_str
                }
            }
            
            logging.info(f"[Chunk {chunk_num}/{num_chunks}] レポート作成リクエスト送信...")
            try:
                response = request_with_retry(
                    'POST',
                    f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
                    headers=headers,
                    data=json.dumps(payload_dict)
                )
                report_id = response.json()["reportId"]
                logging.info(f"[Chunk {chunk_num}/{num_chunks}] レポート作成リクエスト成功 (Report ID: {report_id})")
                
                get_report_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}"
                report_document_id = None
                
                for attempt in range(15):  # 最大15回試行
                    time.sleep(20)
                    response = request_with_retry('GET', get_report_url, headers=headers)
                    status = response.json().get("processingStatus")
                    if status == "DONE":
                        report_document_id = response.json()["reportDocumentId"]
                        break
                    elif status in ["FATAL", "CANCELLED"]:
                        logging.error(f"[Chunk {chunk_num}/{num_chunks}] レポート処理失敗 (Status: {status})")
                        break
                
                if not report_document_id:
                    logging.error(f"[Chunk {chunk_num}/{num_chunks}] タイムアウトまたは失敗")
                    continue
                    
                get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
                response = request_with_retry('GET', get_doc_url, headers=headers)
                download_url = response.json()["url"]
                
                response = request_with_retry('GET', download_url)
                with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                    report_content = f.read()
                logging.info(f"[Chunk {chunk_num}/{num_chunks}] ダウンロード完了")
                
                if report_content.strip():
                    try:
                        json_data = json.loads(report_content)
                        items = json_data.get("dataByAsin", [])
                        
                        if items:
                            chunk_lines = [json.dumps(item, ensure_ascii=False) for item in items]
                            all_ndjson_lines.extend(chunk_lines)
                            logging.info(f"[Chunk {chunk_num}/{num_chunks}] {len(items)}件取得")
                        else:
                            logging.warning(f"[Chunk {chunk_num}/{num_chunks}] データなし")
                    except json.JSONDecodeError:
                        logging.error(f"[Chunk {chunk_num}/{num_chunks}] JSONパース失敗", exc_info=True)
                else:
                    logging.warning(f"[Chunk {chunk_num}/{num_chunks}] コンテンツ空")
            
            except Exception:
                logging.error(f"[Chunk {chunk_num}/{num_chunks}] 処理中にエラー", exc_info=True)
                continue

        if all_ndjson_lines:
            ndjson_content = "\n".join(all_ndjson_lines)
            suffix = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
            blob_name = f"{GCS_FILE_PREFIX}{gcs_folder}/{suffix}.json"
            
            _upload_to_gcs(GCS_BUCKET_NAME, blob_name, ndjson_content)
            logging.info(f"合計 {len(all_ndjson_lines)}件のデータをNDJSON形式で保存しました。")
        else:
            logging.warning("保存対象のデータがありませんでした。")

        logging.info("=== Brand Analytics Search Query Performance Report (WEEK) 処理完了 ===")

    except Exception:
        logging.critical("Brand Analytics Search Query Performance Report (WEEK) 処理中に致命的なエラーが発生しました", exc_info=True)
        raise
