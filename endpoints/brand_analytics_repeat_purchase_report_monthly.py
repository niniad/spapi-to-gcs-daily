"""
Brand Analytics Repeat Purchase Report (Monthly) Module

このモジュールは、SP-APIのBrand Analytics Repeat Purchase Report (GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT) を取得し、GCSに保存します。
月次 (MONTH) のデータを取得します。

- レポートタイプ: GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT
- 期間: MONTH (直近の完了した月)
- 保存形式: JSON
- 保存先: brand-analytics-repeat-purchase/monthly/YYYYMM.json
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


# ===================================================================
# 設定
# ===================================================================
MARKETPLACE_ID = "A1VC38T7YXB528"  # 日本
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
REPORT_TYPE = "GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT"
GCS_BUCKET_NAME = "sp-api-bucket"
GCS_FILE_PREFIX = "brand-analytics-repeat-purchase/monthly/"


def _upload_to_gcs(bucket_name, blob_name, content):
    """GCSにファイルをアップロード"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content, content_type='application/x-ndjson')
        logging.info(f"GCSへの保存成功: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        logging.error(f"GCSへのアップロードに失敗しました: {e}", exc_info=True)


def run():
    logging.info("=== Brand Analytics Repeat Purchase Report (MONTH) 処理開始 ===")
    
    try:
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': access_token
        }
        
        # データ取得期間の計算 (先月)
        utc_now = datetime.now(timezone.utc)
        
        # 先月の初日と末日を計算
        # 今月の1日の1日前が、先月の末日
        this_month_first = utc_now.replace(day=1)
        last_month_last = this_month_first - timedelta(days=1)
        last_month_first = last_month_last.replace(day=1)
        
        start_date_str = last_month_first.strftime('%Y-%m-%d')
        end_date_str = last_month_last.strftime('%Y-%m-%d')
        
        logging.info(f"対象期間 (MONTH): {start_date_str} 〜 {end_date_str}")
        
        # レポート作成リクエスト
        payload_dict = {
            "marketplaceIds": [MARKETPLACE_ID],
            "reportType": REPORT_TYPE,
            "dataStartTime": start_date_str,
            "dataEndTime": end_date_str,
            "reportOptions": {
                "reportPeriod": "MONTH"
            }
        }
        
        response = request_with_retry(
            'POST',
            f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
            headers=headers,
            data=json.dumps(payload_dict)
        )
        report_id = response.json()["reportId"]
        logging.info(f"レポート作成リクエスト成功 (Report ID: {report_id})")
        
        # ポーリング
        processing_status = "IN_PROGRESS"
        report_document_id = None
        
        for _ in range(30): # 最大10分
            time.sleep(20)
            resp = request_with_retry(
                'GET',
                f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}",
                headers=headers
            )
            data = resp.json()
            processing_status = data.get("processingStatus")
            
            if processing_status == "DONE":
                report_document_id = data.get("reportDocumentId")
                logging.info("レポート作成完了 (DONE)")
                break
            elif processing_status in ["FATAL", "CANCELLED"]:
                logging.error(f"レポート処理失敗 Status: {processing_status}")
                return
            else:
                logging.info(f"処理中... ({processing_status})")
        
        if not report_document_id:
            logging.error("Timeout: レポート作成が完了しませんでした。")
            return

        # ダウンロードURL取得
        resp = request_with_retry('GET', f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}", headers=headers)
        download_url = resp.json()["url"]
        
        # ダウンロード
        resp = request_with_retry('GET', download_url)
        
        # 解凍とデコード
        content = None
        try:
            with gzip.open(io.BytesIO(resp.content), 'rt', encoding='utf-8') as f:
                content = f.read()
        except gzip.BadGzipFile:
            content = resp.content.decode('utf-8')
            
        if content:
            # JSONL形式に変換
            try:
                json_data = json.loads(content)
                items = json_data.get("dataByAsin", [])
                
                if items:
                    jsonl_lines = [json.dumps(item, ensure_ascii=False) for item in items]
                    jsonl_content = "\n".join(jsonl_lines)
                    
                    # GCS保存 (ファイル名は年月)
                    filename_suffix = last_month_first.strftime('%Y%m')
                    blob_name = f"{GCS_FILE_PREFIX}{filename_suffix}.jsonl"
                    _upload_to_gcs(GCS_BUCKET_NAME, blob_name, jsonl_content)
                else:
                     logging.warning("データ(dataByAsin)が含まれていません。")
            except json.JSONDecodeError:
                logging.error("JSONのパースに失敗しました。", exc_info=True)
        else:
            logging.warning("コンテンツが空でした。")
            
    except Exception as e:
        logging.critical(f"Brand Analytics Repeat Purchase (MONTH) 処理中にエラー: {e}", exc_info=True)
        raise

