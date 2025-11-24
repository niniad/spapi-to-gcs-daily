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
        
        # 日付ごとにループ処理
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\n[{date_str}] の処理を開始...")
            
            # 各レポート種別を処理
            for config in REPORT_CONFIGS:
                print(f"  -> レポート種別: [{config['type']}] の処理を開始...")
                
                try:
                    # レポート作成リクエスト
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
                    print(f"    -> レポート作成リクエスト成功 (Report ID: {report_id})")
                    
                    # レポート完了を待機(ポーリング)
                    get_report_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}"
                    report_document_id = None
                    
                    for attempt in range(15):  # 最大15回試行(約5分)
                        time.sleep(20)  # 20秒待機
                        response = request_with_retry(
                            'GET',
                            get_report_url,
                            headers=headers
                        )
                        status = response.json().get("processingStatus")
                        
                        if status == "DONE":
                            report_document_id = response.json()["reportDocumentId"]
                            print(f"    -> レポート作成完了 (DONE)")
                            break
                        elif status in ["FATAL", "CANCELLED"]:
                            print(f"    -> Warn: レポート処理が失敗またはキャンセル (Status: {status})")
                            break
                        else:
                            print(f"    -> レポート作成中 (Status: {status})...")
                    
                    if not report_document_id:
                        print(f"    -> Warn: レポート処理がタイムアウト。スキップします。")
                        continue
                    
                    # レポートドキュメントのダウンロードURL取得
                    get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
                    response = request_with_retry('GET', get_doc_url, headers=headers)
                    download_url = response.json()["url"]
                    
                    # レポートをダウンロードして解凍
                    response = request_with_retry('GET', download_url)
                    with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                        report_content = f.read()
                    print(f"    -> レポートのダウンロードと解凍が完了。")
                    
                    # GCSに保存(内容が空でない場合のみ)
                    if report_content.strip():
                        blob_name = f"{config['gcs_file_prefix']}{current_date.strftime('%Y%m%d')}.json"
                        _upload_to_gcs(config['gcs_bucket_name'], blob_name, report_content)
                    else:
                        print("    -> レポート内容が空のためスキップ。")
                
                except Exception as e:
                    print(f"    -> Error: [{config['type']}] の処理中にエラー発生: {e}")
                    continue
                
                finally:
                    time.sleep(2)  # 次のリクエストまで少し待機
            
            # 次の日付へ
            current_date += timedelta(days=1)
        
        print("\n=== Sales and Traffic Report 処理完了 ===")
        
    except Exception as e:
        print(f"Error: Sales and Traffic Report処理中に致命的なエラーが発生しました: {e}")
        raise
