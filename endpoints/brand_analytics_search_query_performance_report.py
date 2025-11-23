"""
Brand Analytics Search Query Performance Report Module

このモジュールは、SP-APIのBrand Analytics Search Query Performance Reportを取得し、GCSに保存します。
- 取得期間: 直近の完全な1週間（日曜日〜土曜日）
- 頻度: 毎日実行（期間が重複する場合は上書き）
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
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
GCS_BUCKET_NAME = "sp-api-brand-analytics-search-query-performance-report"
GCS_FILE_PREFIX = "sp-api-brand-analytics-search-query-performance-report-"

# 対象ASINリスト
ASIN_LIST = [
    "B0D894LS44", "B0D89H2L67", "B0D89DTD29", "B0D88XNCHG", "B0DBSM5ZDZ",
    "B0DBSF1CZ6", "B0DBS2WWJN", "B0DBS1ZQ7K", "B0DBS2CK1T", "B0DBSB6XY9",
    "B0DT5P24N2", "B0DT51B33M", "B0FRZ3Z755", "B0FRZ2D3G2"
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
        print(f"  -> GCSへの保存成功: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f"  -> Error: GCSへのアップロードに失敗しました: {e}")


def _get_last_complete_week_range():
    """
    直近の完全な1週間（日曜日〜土曜日）の開始日と終了日を計算します。
    
    Returns:
        tuple: (start_date, end_date) datetime objects
    """
    utc_now = datetime.now(timezone.utc)
    
    # 今日の曜日を取得 (月=0, ..., 土=5, 日=6)
    weekday = utc_now.weekday()
    
    # 直近の土曜日までの日数を計算
    # 月(0) -> 2日前 (0-5)%7 = 2
    # 日(6) -> 1日前 (6-5)%7 = 1
    # 土(5) -> 7日前 (5-5)%7 = 0 -> 7 (当日を含めないため)
    days_since_saturday = (weekday - 5) % 7
    if days_since_saturday == 0:
        days_since_saturday = 7
        
    end_date = utc_now - timedelta(days=days_since_saturday)
    start_date = end_date - timedelta(days=6)
    
    # 時間を合わせる (Start: 00:00:00, End: 00:00:00? API仕様によるが、通常日付のみでよいか、あるいはT00:00:00Z形式)
    # ここではdatetimeオブジェクトを返す
    return start_date, end_date


def run():
    """
    Brand Analytics Search Query Performance Reportの取得とGCS保存を実行します。
    """
    print("\n=== Brand Analytics Search Query Performance Report 処理開始 ===")
    
    try:
        # アクセストークン取得
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': access_token
        }
        
        # データ取得期間を計算
        start_date, end_date = _get_last_complete_week_range()
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        print(f"データ取得期間: {start_date_str} (日) から {end_date_str} (土)")
        
        # レポート作成リクエスト
        # dataStartTime/EndTimeは日付の開始時点を指定
        payload_dict = {
            "marketplaceIds": [MARKETPLACE_ID],
            "reportType": "GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT",
            "dataStartTime": f"{start_date_str}T00:00:00Z",
            "dataEndTime": f"{end_date_str}T00:00:00Z", # EndTimeは通常その日の始まりか終わりか？サンプルはT00:00:00Z
            "reportOptions": {
                "reportPeriod": "WEEK",
                "asin": " ".join(ASIN_LIST)
            }
        }
        
        payload = json.dumps(payload_dict)
        
        print("  -> レポート作成リクエスト送信...")
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
        
        for attempt in range(15):  # 最大15回試行
            time.sleep(20)
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
            print(f"    -> Warn: レポート処理がタイムアウトしました。")
            return

        # レポートドキュメントのダウンロードURL取得
        get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
        response = request_with_retry('GET', get_doc_url, headers=headers)
        download_url = response.json()["url"]
        
        # レポートをダウンロードして解凍
        response = request_with_retry('GET', download_url)
        with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
            report_content = f.read()
        print(f"    -> レポートのダウンロードと解凍が完了。")
        
        # GCSに保存
        if report_content.strip():
            # BigQueryの外部テーブル(JSONL)に対応するため、NDJSON形式に変換
            try:
                json_data = json.loads(report_content)
                items = json_data.get("dataByAsin", [])
                
                if items:
                    ndjson_lines = [json.dumps(item, ensure_ascii=False) for item in items]
                    ndjson_content = "\n".join(ndjson_lines)
                    
                    # ファイル名: prefix-YYYYMMDD-YYYYMMDD.json
                    blob_name = f"{GCS_FILE_PREFIX}{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}.json"
                    _upload_to_gcs(GCS_BUCKET_NAME, blob_name, ndjson_content)
                    print(f"    -> {len(items)}件のデータをNDJSON形式で保存しました。")
                else:
                    print("    -> データ(dataByAsin)が存在しないためスキップ。")
            
            except json.JSONDecodeError as e:
                print(f"    -> Error: JSONのパースに失敗しました: {e}")
                # フォールバック: 生データを保存（デバッグ用）
                blob_name = f"{GCS_FILE_PREFIX}raw-{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}.json"
                _upload_to_gcs(GCS_BUCKET_NAME, blob_name, report_content)
        else:
            print("    -> レポート内容が空のためスキップ。")

        print("\n=== Brand Analytics Search Query Performance Report 処理完了 ===")

    except Exception as e:
        print(f"Error: Brand Analytics Search Query Performance Report処理中にエラーが発生しました: {e}")
        raise
