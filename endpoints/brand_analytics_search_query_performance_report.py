"""
Brand Analytics Search Query Performance Report Module

このモジュールは、SP-APIのBrand Analytics Search Query Performance Reportを取得し、GCSに保存します。
- 取得期間: 直近の完全な1週間（日曜日〜土曜日）
- 頻度: 毎日実行（期間が重複する場合は上書き）
- ASINリスト: FBA Inventory APIから自動取得
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


def _get_previous_month_range():
    """
    先月の初日と末日を計算します。
    
    Returns:
        tuple: (start_date, end_date) datetime objects
    """
    utc_now = datetime.now(timezone.utc)
    # 今月の1日
    this_month_first = utc_now.replace(day=1)
    # 先月の末日 = 今月の1日の1日前
    last_month_last = this_month_first - timedelta(days=1)
    # 先月の1日
    last_month_first = last_month_last.replace(day=1)
    
    return last_month_first, last_month_last


def run():
    """
    Brand Analytics Search Query Performance Reportの取得とGCS保存を実行します。
    WEEK(週次)とMONTH(月次)の両方を取得します。
    """
    print("\n=== Brand Analytics Search Query Performance Report 処理開始 ===")
    
    try:
        # アクセストークン取得
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': access_token
        }

        # ASIN一覧を取得
        print("\n[ASIN一覧取得] FBA Inventoryから取得中...")
        asin_list = get_asin_list()
        print(f"  -> 取得ASIN数: {len(asin_list)}")

        # レポート取得設定
        report_configs = [
            {
                "period": "WEEK",
                "get_range_func": _get_last_complete_week_range,
                "gcs_folder": "WEEK",
                "filename_suffix_fmt": "week-%Y%m%d-%Y%m%d" # start-end
            },
            {
                "period": "MONTH",
                "get_range_func": _get_previous_month_range,
                "gcs_folder": "MONTH",
                "filename_suffix_fmt": "month-%Y%m" # month only
            }
        ]
        
        for config in report_configs:
            period = config["period"]
            print(f"\n--- {period} レポート処理開始 ---")
            
            # データ取得期間を計算
            start_date, end_date = config["get_range_func"]()
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            print(f"データ取得期間: {start_date_str} から {end_date_str}")
            
            # レポート作成リクエスト
            payload_dict = {
                "marketplaceIds": [MARKETPLACE_ID],
                "reportType": "GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT",
                "dataStartTime": f"{start_date_str}T00:00:00Z",
                "dataEndTime": f"{end_date_str}T00:00:00Z",
                "reportOptions": {
                    "reportPeriod": period,
                    "asin": " ".join(asin_list)  # 動的に取得したASINリストを使用
                }
            }
            
            payload = json.dumps(payload_dict)
            
            print("  -> レポート作成リクエスト送信...")
            try:
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
                    print(f"    -> Warn: レポート処理がタイムアウトしました。スキップします。")
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
                
                # GCSに保存
                if report_content.strip():
                    # BigQueryの外部テーブル(JSONL)に対応するため、NDJSON形式に変換
                    try:
                        json_data = json.loads(report_content)
                        items = json_data.get("dataByAsin", [])
                        
                        if items:
                            ndjson_lines = [json.dumps(item, ensure_ascii=False) for item in items]
                            ndjson_content = "\n".join(ndjson_lines)
                            
                            # ファイル名生成
                            # WEEK: sp-api-brand-analytics-search-query-performance-report-week-yyyymmdd-yyyymmdd.json
                            # MONTH: sp-api-brand-analytics-search-query-performance-report-month-yyyymm.json
                            if period == "WEEK":
                                suffix = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
                            else: # MONTH
                                suffix = f"{start_date.strftime('%Y%m')}"
                            
                            blob_name = f"{GCS_FILE_PREFIX}{config['gcs_folder']}/{suffix}.json"
                            
                            _upload_to_gcs(GCS_BUCKET_NAME, blob_name, ndjson_content)
                            print(f"    -> {len(items)}件のデータをNDJSON形式で保存しました。")
                        else:
                            print("    -> データ(dataByAsin)が存在しないためスキップ。")
                    
                    except json.JSONDecodeError as e:
                        print(f"    -> Error: JSONのパースに失敗しました: {e}")
                        # フォールバック
                        if period == "WEEK":
                            suffix = f"week-raw-{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
                        else:
                            suffix = f"month-raw-{start_date.strftime('%Y%m')}"
                        blob_name = f"{GCS_FILE_PREFIX}{config['gcs_folder']}/{suffix}.json"
                        _upload_to_gcs(GCS_BUCKET_NAME, blob_name, report_content)
                else:
                    print("    -> レポート内容が空のためスキップ。")
            
            except Exception as e:
                print(f"    -> Error: {period} レポート処理中にエラーが発生: {e}")
                continue

        print("\n=== Brand Analytics Search Query Performance Report 処理完了 ===")

    except Exception as e:
        print(f"Error: Brand Analytics Search Query Performance Report処理中にエラーが発生しました: {e}")
        raise
