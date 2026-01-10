"""
Ledger Detail View Data Report Module

このモジュールは、SP-APIのGET_LEDGER_DETAIL_VIEW_DATAレポートを取得し、GCSに保存します。
- 取得期間: 前月（月初から月末まで）
- 頻度: 毎日実行（前月のデータを上書き）
- 保存形式: TSV (Tab-Separated Values)
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
GCS_BUCKET_NAME = "sp-api-bucket"
GCS_FILE_PREFIX = "ledger-detail-view-data/"


def _upload_to_gcs(bucket_name, blob_name, content):
    """
    GCSにファイルをアップロードします。
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content, content_type='text/tab-separated-values')
        logging.info(f"GCSへの保存成功: gs://{bucket_name}/{blob_name}")
    except Exception:
        logging.error(f"GCSへのアップロードに失敗しました: gs://{bucket_name}/{blob_name}", exc_info=True)


def _get_previous_month_range():
    """
    データ取得対象の期間を計算します。
    """
    utc_now = datetime.now(timezone.utc)
    this_month_first = utc_now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_month_last = this_month_first - timedelta(days=1)
    last_month_first = last_month_last.replace(day=1)
    return last_month_first, last_month_last


def run():
    """
    Ledger Detail View Data Reportの取得とGCS保存を実行します。
    """
    logging.info("=== Ledger Detail View Data Report 処理開始 ===")
    
    try:
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': access_token
        }
        
        start_date, end_date = _get_previous_month_range()
        
        start_date_str = start_date.strftime('%Y-%m-%dT00:00:00+09:00')
        end_date_str = end_date.strftime('%Y-%m-%dT23:59:59+09:00')
        
        logging.info(f"データ取得期間: {start_date.strftime('%Y-%m-%d')} から {end_date.strftime('%Y-%m-%d')}")
        
        try:
            payload_dict = {
                "marketplaceIds": [MARKETPLACE_ID],
                "reportType": "GET_LEDGER_DETAIL_VIEW_DATA",
                "dataStartTime": start_date_str,
                "dataEndTime": end_date_str
            }
            
            logging.info("レポート作成リクエスト送信...")
            response = request_with_retry(
                'POST',
                f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
                headers=headers,
                data=json.dumps(payload_dict)
            )
            report_id = response.json()["reportId"]
            logging.info(f"レポート作成リクエスト成功 (Report ID: {report_id})")
            
            get_report_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}"
            report_document_id = None
            
            for attempt in range(20):
                time.sleep(30) # 30秒ごとに変更
                response = request_with_retry('GET', get_report_url, headers=headers)
                status = response.json().get("processingStatus")
                
                if status == "DONE":
                    report_document_id = response.json()["reportDocumentId"]
                    logging.info("レポート作成完了 (DONE)")
                    break
                elif status in ["FATAL", "CANCELLED"]:
                    logging.warning(f"レポート処理が失敗またはキャンセル (Status: {status})")
                    break
                else:
                    logging.info(f"レポート作成中 (Status: {status})...")
            
            if not report_document_id:
                logging.warning("レポート処理がタイムアウトしました。スキップします。")
                return
            
            get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
            response = request_with_retry('GET', get_doc_url, headers=headers)
            download_url = response.json()["url"]
            
            response = request_with_retry('GET', download_url)
            
            report_content = None
            try:
                # GZIP解凍を試みる
                with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                    report_content = f.read()
                logging.info("GZIP (UTF-8)で解凍・デコード完了。")
            except (gzip.BadGzipFile, UnicodeDecodeError) as e:
                logging.warning(f"GZIP(UTF-8)解凍/デコード失敗({e})。CP932で試行...")
                try:
                    report_content = response.content.decode('cp932')
                    logging.info("CP932でデコード完了。")
                except UnicodeDecodeError:
                    logging.warning("CP932デコード失敗。latin-1で試行...")
                    report_content = response.content.decode('latin-1')
                    logging.info("latin-1でデコード完了。")
            
            logging.info("レポートのダウンロード完了。")
            
            if report_content and report_content.strip():
                blob_name = f"{GCS_FILE_PREFIX}{start_date.strftime('%Y%m')}.tsv"
                _upload_to_gcs(GCS_BUCKET_NAME, blob_name, report_content)
            else:
                logging.warning("レポート内容が空のためスキップ。")
        
        except Exception:
            logging.error("レポート処理中にエラー発生", exc_info=True)
            raise

        logging.info("=== Ledger Detail View Data Report 処理完了 ===")

    except Exception:
        logging.critical("Ledger Detail View Data Report処理中に致命的なエラーが発生しました", exc_info=True)
        raise
