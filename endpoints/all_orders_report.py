"""
All Orders Report Module

このモジュールは、SP-APIのAll Orders Report (GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL) を取得し、GCSに保存します。
過去1週間分のデータを日次で取得し、GCS上のファイルを上書き更新します。
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
START_DAYS_AGO = 8  # 8日前のデータから取得 (今日を含まず過去7日間)
END_DAYS_AGO = 1    # 1日前のデータまで取得
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
REPORT_TYPE = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL"
GCS_BUCKET_NAME = "sp-api-bucket"
GCS_FILE_PREFIX = "all-orders-report/"


def _upload_to_gcs(bucket_name, blob_name, content):
    """
    GCSにファイルをアップロードします。
    
    Args:
        bucket_name: GCSバケット名
        blob_name: 保存するファイル名
        content: ファイルの内容
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        # TSVファイルとして保存 (Content-Typeはtext/tab-separated-values推奨だが、扱いやすさのためtext/plainでも可)
        blob.upload_from_string(content, content_type='text/tab-separated-values; charset=utf-8')
        logging.info(f"GCSへの保存成功: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        logging.error(f"GCSへのアップロードに失敗しました: {e}", exc_info=True)


def run():
    """
    All Orders Reportの取得とGCS保存を実行します。
    
    処理フロー:
    1. SP-APIアクセストークンを取得
    2. 指定期間（過去1週間）の各日付について:
       - レポートを作成・取得
       - GCSに保存 (同名ファイルは上書き)
    """
    logging.info("=== All Orders Report 処理開始 ===")
    
    try:
        # アクセストークン取得
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': access_token
        }
        
        # データ取得期間を計算
        utc_now = datetime.now(timezone.utc)
        start_date = utc_now - timedelta(days=START_DAYS_AGO)
        end_date = utc_now - timedelta(days=END_DAYS_AGO)
        logging.info(f"データ取得期間: {start_date.strftime('%Y-%m-%d')} から {end_date.strftime('%Y-%m-%d')}")
        
        # 1. レポート作成リクエストを一括送信
        pending_reports = []
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            logging.info(f"[{date_str}] のレポート作成をリクエスト中...")
            
            try:
                # レポート作成リクエスト
                payload_dict = {
                    "marketplaceIds": [MARKETPLACE_ID],
                    "reportType": REPORT_TYPE,
                    "dataStartTime": f"{date_str}T00:00:00Z",
                    "dataEndTime": f"{date_str}T23:59:59Z",
                }
                
                payload = json.dumps(payload_dict)
                response = request_with_retry(
                    'POST',
                    f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
                    headers=headers,
                    data=payload
                )
                report_id = response.json()["reportId"]
                logging.info(f"Request OK (Report ID: {report_id})")
                
                pending_reports.append({
                    "report_id": report_id,
                    "date_str": date_str,
                    "current_date": current_date
                })
                
                # Rate Limit対策 (Burst 15)
                time.sleep(2)
                
            except Exception as e:
                logging.error(f"[{date_str}] リクエスト失敗: {e}", exc_info=True)

            current_date += timedelta(days=1)

        # 2. レポート完了待機とダウンロード
        logging.info(f"--- レポート生成待ち (対象: {len(pending_reports)}件) ---")
        
        max_loops = 40 # 30s * 40 = 20 mins
        completed_reports = []
        
        for i in range(max_loops):
            if len(completed_reports) == len(pending_reports):
                logging.info("全てのレポート処理が完了しました。")
                break
                
            logging.info(f"ステータス確認 (試行 {i+1}/{max_loops})...")
            
            for item in pending_reports:
                report_id = item['report_id']
                if report_id in completed_reports:
                    continue
                
                try:
                    get_report_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}"
                    response = request_with_retry('GET', get_report_url, headers=headers)
                    status = response.json().get("processingStatus")
                    
                    if status == "DONE":
                        logging.info(f"Report {report_id} ({item['date_str']}): DONE")
                        report_document_id = response.json()["reportDocumentId"]
                        
                        # ダウンロード処理
                        get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
                        doc_response = request_with_retry('GET', get_doc_url, headers=headers)
                        download_url = doc_response.json()["url"]
                        
                        dl_response = request_with_retry('GET', download_url)
                        
                        content_to_save = None
                        try:
                            with gzip.open(io.BytesIO(dl_response.content), 'rt', encoding='utf-8') as f:
                                content_to_save = f.read()
                            logging.info("GZIP解凍完了")
                        except gzip.BadGzipFile:
                            try:
                                content_to_save = dl_response.content.decode('utf-8')
                            except UnicodeDecodeError:
                                try:
                                    content_to_save = dl_response.content.decode('cp932')
                                    logging.info("Shift_JIS(cp932)でデコードしました")
                                except UnicodeDecodeError:
                                    content_to_save = dl_response.content.decode('latin-1')

                        # GCS保存
                        if content_to_save and content_to_save.strip():
                            blob_name = f"{GCS_FILE_PREFIX}{item['current_date'].strftime('%Y%m%d')}.tsv"
                            _upload_to_gcs(GCS_BUCKET_NAME, blob_name, content_to_save)
                        else:
                            logging.warning("内容が空のため保存スキップ")
                            
                        completed_reports.append(report_id)
                        
                    elif status in ["FATAL", "CANCELLED"]:
                        logging.warning(f"Report {report_id}: Failed ({status})")
                        completed_reports.append(report_id)

                except Exception as e:
                    logging.error(f"Report {report_id} 処理中にエラー", exc_info=True)
                    # このレポートは処理済みとしてマークし、ループを続ける
                    completed_reports.append(report_id)
            
            if len(completed_reports) == len(pending_reports):
                break
            
            if len(pending_reports) > len(completed_reports):
                time.sleep(30)
        
        logging.info("=== All Orders Report 処理完了 ===")
        
    except Exception as e:
        logging.critical("All Orders Report処理中に致命的なエラーが発生しました", exc_info=True)
        raise
