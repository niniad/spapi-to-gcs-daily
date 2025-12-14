"""
Brand Analytics Repeat Purchase Report (Weekly) Module

このモジュールは、SP-APIのBrand Analytics Repeat Purchase Report (GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT) を取得し、GCSに保存します。
週次 (WEEK) のデータを取得します。

- レポートタイプ: GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT
- 期間: WEEK (直近の完了した週)
- 保存形式: JSON
- 保存先: brand-analytics-repeat-purchase/weekly/YYYYMMDD.json
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
REPORT_TYPE = "GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT"
GCS_BUCKET_NAME = "sp-api-bucket"
GCS_FILE_PREFIX = "brand-analytics-repeat-purchase/weekly/"


def _upload_to_gcs(bucket_name, blob_name, content):
    """GCSにファイルをアップロード"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content, content_type='application/x-ndjson')
        print(f"  -> GCSへの保存成功: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f"  -> Error: GCSへのアップロードに失敗しました: {e}")


def run():
    print("\n=== Brand Analytics Repeat Purchase Report (WEEK) 処理開始 ===")
    
    try:
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': access_token
        }
        
        # データ取得期間の計算 (先週の日曜日〜土曜日)
        # Brand Analyticsの週次レポートは 日曜始まり・土曜終わり
        utc_now = datetime.now(timezone.utc)
        
        # 今日から見て「直近の土曜」とその「前の日曜」を特定する
        # weekday(): 月=0, 火=1, ..., 土=5, 日=6
        # もし今日が日曜(6)なら、先週の土曜(5)は昨日。
        today_weekday = utc_now.weekday() # 0-6
        days_since_saturday = (today_weekday + 1) % 7 + 1 # +1 for safely range, adjust later
        
        # 確実に完了している週を取得するため、2週間前のデータ取得を狙うか、
        # あるいは「先週」を取得するか。
        # AmazonのBAデータは確定に時間がかかる（24-48時間後）ため、3日前の土曜終了データなら安全。
        
        # シンプルに: 8日前を基準日とし、その基準日が含まれる週(日曜〜土曜)を対象にする
        # これなら常に「完了した直近の週」になるはず。
        target_date = utc_now - timedelta(days=8)
        
        # target_dateの曜日に基づいて、その週の日曜を算出
        # weekday(): 月=0 ... 日=6
        # Pythonのweekdayは月曜始まり。
        # target_dateが日曜(6)なら offset=0, 月曜(0)なら offset=1...
        # Amazon週次は日曜開始。
        # target_dateの曜日(0-6)から、その週の日曜日は...
        # (target_date.weekday() + 1) % 7 が「日曜から何日目か(日曜=0, 月曜=1...)」
        days_from_sunday = (target_date.weekday() + 1) % 7
        report_start_date = target_date - timedelta(days=days_from_sunday)
        report_end_date = report_start_date + timedelta(days=6)
        
        start_date_str = report_start_date.strftime('%Y-%m-%d')
        end_date_str = report_end_date.strftime('%Y-%m-%d')
        
        print(f"対象期間 (WEEK): {start_date_str} 〜 {end_date_str}")
        
        # レポート作成リクエスト
        payload_dict = {
            "marketplaceIds": [MARKETPLACE_ID],
            "reportType": REPORT_TYPE,
            "dataStartTime": start_date_str,
            "dataEndTime": end_date_str,
            "reportOptions": {
                "reportPeriod": "WEEK"
            }
        }
        
        response = request_with_retry(
            'POST',
            f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
            headers=headers,
            data=json.dumps(payload_dict)
        )
        report_id = response.json()["reportId"]
        print(f"  -> レポート作成リクエスト成功 (Report ID: {report_id})")
        
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
                print("  -> レポート作成完了 (DONE)")
                break
            elif processing_status in ["FATAL", "CANCELLED"]:
                print(f"  -> Error: レポート処理失敗 Status: {processing_status}")
                return
            else:
                print(f"  -> 処理中... ({processing_status})")
        
        if not report_document_id:
            print("  -> Timeout: レポート作成が完了しませんでした。")
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

                    # GCS保存 (ファイル名は期間終了日)
                    blob_name = f"{GCS_FILE_PREFIX}{end_date_str.replace('-', '')}.jsonl"
                    _upload_to_gcs(GCS_BUCKET_NAME, blob_name, jsonl_content)
                else:
                    print("  -> データ(dataByAsin)が含まれていません。")
            except json.JSONDecodeError:
                print("  -> Error: JSONのパースに失敗しました。")
        else:
            print("  -> コンテンツが空でした。")
            
    except Exception as e:
        print(f"Error: Brand Analytics Repeat Purchase (WEEK) 処理中にエラー: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run()
