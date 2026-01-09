"""
Sales and Traffic Report Module

このモジュールは、SP-APIのSales and Traffic Reportを取得し、GCSに保存します。
- DAYレポート: 日次の売上とトラフィックデータ
- CHILD ASINレポート: 子ASINごとの売上とトラフィックデータ
"""

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
START_DAYS_AGO = 8  # 8日前のデータから取得
END_DAYS_AGO = 1    # 1日前のデータまで取得
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"

# レポート種別ごとの設定
REPORT_CONFIGS = [
    {
        "type": "DAY",
        "gcs_bucket_name": "sp-api-bucket",
        "gcs_file_prefix": "sales-and-traffic-report/day/",
        "report_options": {}
    },
    {
        "type": "CHILD ASIN",
        "gcs_bucket_name": "sp-api-bucket",
        "gcs_file_prefix": "sales-and-traffic-report/child-asin/",
        "report_options": {
            "dateGranularity": "DAY",
            "asinGranularity": "CHILD"
        }
    }
]


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
        blob.upload_from_string(content, content_type='application/json')
        print(f"  -> GCSへの保存成功: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f"  -> Error: GCSへのアップロードに失敗しました: {e}")


def run():
    """
    Sales and Traffic Reportの取得とGCS保存を実行します。
    
    処理フロー:
    1. SP-APIアクセストークンを取得
    2. 指定期間の各日付について:
       - DAYレポートを作成・取得・保存
       - CHILD ASINレポートを作成・取得・保存
    """
    print("\n=== Sales and Traffic Report 処理開始 ===")
    
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
        print(f"データ取得期間: {start_date.strftime('%Y-%m-%d')} から {end_date.strftime('%Y-%m-%d')}")
        
        # 1. レポート作成リクエストを一括送信
        pending_reports = []  # list of triggering info: {'report_id': ..., 'config': ..., 'date': ...}
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\n[{date_str}] のレポート作成をリクエスト中...")
            
            for config in REPORT_CONFIGS:
                try:
                    payload_dict = {
                        "marketplaceIds": [MARKETPLACE_ID],
                        "reportType": "GET_SALES_AND_TRAFFIC_REPORT",
                        "dataStartTime": f"{date_str}T00:00:00Z",
                        "dataEndTime": f"{date_str}T23:59:59Z",
                    }
                    if config["report_options"]:
                        payload_dict["reportOptions"] = config["report_options"]
                    
                    payload = json.dumps(payload_dict)
                    response = request_with_retry(
                        'POST',
                        f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
                        headers=headers,
                        data=payload
                    )
                    report_id = response.json()["reportId"]
                    print(f"  -> [{config['type']}] Request OK (Report ID: {report_id})")
                    
                    pending_reports.append({
                        "report_id": report_id,
                        "config": config,
                        "date_str": date_str,
                        "current_date": current_date # object for filename generation if needed
                    })
                    
                    # Rate Limit対策 (Burst 15, Rate 0.0167/hz)
                    # 連続して投げすぎると429になる可能性があるため少し待機
                    time.sleep(2)

                except Exception as e:
                    print(f"  -> Error: [{config['type']}] リクエスト失敗: {e}")
            
            current_date += timedelta(days=1)


        # 2. レポート完了待機とダウンロード
        print(f"\n--- レポート生成待ち (対象: {len(pending_reports)}件) ---")
        
        # 全て完了するまでループ (最大試行回数は全体時間で管理するか、個別に管理するか。ここではシンプルにループ回数でガード)
        # 合計待機時間が極端に長くならないように注意
        max_loops = 40 # 30秒 * 40 = 20分 (Cloud Run 60分設定なら余裕)
        
        completed_reports = [] # 完了したレポートID
        
        for i in range(max_loops):
            if len(completed_reports) == len(pending_reports):
                print("全てのレポート処理が完了しました。")
                break
                
            print(f"\nステータス確認 (試行 {i+1}/{max_loops})...")
            all_done_this_loop = True
            
            for item in pending_reports:
                report_id = item['report_id']
                if report_id in completed_reports:
                    continue
                
                try:
                    get_report_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}"
                    response = request_with_retry('GET', get_report_url, headers=headers)
                    status = response.json().get("processingStatus")
                    
                    if status == "DONE":
                        print(f"  -> Report {report_id} ({item['date_str']} {item['config']['type']}): DONE")
                        
                        # ダウンロード処理
                        report_document_id = response.json()["reportDocumentId"]
                        get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
                        doc_response = request_with_retry('GET', get_doc_url, headers=headers)
                        download_url = doc_response.json()["url"]
                        
                        dl_response = request_with_retry('GET', download_url)
                        with gzip.open(io.BytesIO(dl_response.content), 'rt', encoding='utf-8') as f:
                            report_content = f.read()
                        
                        # GCS保存
                        if report_content.strip():
                            blob_name = f"{item['config']['gcs_file_prefix']}{item['current_date'].strftime('%Y%m%d')}.json"
                            _upload_to_gcs(item['config']['gcs_bucket_name'], blob_name, report_content)
                        else:
                            print("    -> Warn: 内容が空のため保存スキップ")
                        
                        completed_reports.append(report_id)
                        
                    elif status in ["FATAL", "CANCELLED"]:
                        print(f"  -> Report {report_id} ({item['date_str']}): Failed ({status})")
                        completed_reports.append(report_id) # 失敗扱いとして完了リストに入れる(再試行しない)
                    else:
                        # PROCESSING, IN_QUEUE
                        all_done_this_loop = False
                        # print(f"  -> Report {report_id}: {status}") # ログ過多になるので省略可
                
                except Exception as e:
                    print(f"  -> Error: Report {report_id} 処理中にエラー: {e}")
                    # エラーでも一時的なAPIエラーならリトライしたいが、ここではログ出して次へ
            
            if len(completed_reports) == len(pending_reports):
                break
                
            if not all_done_this_loop:
                time.sleep(30) # 30秒待機

        print("\n=== Sales and Traffic Report 処理完了 ===")
        
    except Exception as e:
        print(f"Error: Sales and Traffic Report処理中に致命的なエラーが発生しました: {e}")
        raise
