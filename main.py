import os
import json
import time
import gzip
import io
import requests
from datetime import datetime, timedelta, timezone
from google.cloud import storage

# ===================================================================
# ユーザー設定
# ===================================================================
# SP-APIのマーケットプレイスID (日本: A1VC38T7YXB528)
MARKETPLACE_ID = "A1VC38T7YXB528"

# データ取得期間 (本日を基準)
START_DAYS_AGO = 8 # 8日前のデータから取得
END_DAYS_AGO = 1   # 1日前のデータまで取得

# SP-APIのエンドポイント
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"

# ★★★ レポート種別ごとの設定 ★★★
REPORT_CONFIGS = [
    {
        "type": "DAY",
        "gcs_bucket_name": "sp-api-sales-and-traffic-report-day",
        "gcs_file_prefix": "sp-api-sales-and-traffic-report-day-",
        "report_options": {}
    },
    {
        "type": "CHILD ASIN",
        "gcs_bucket_name": "sp-api-sales-and-traffic-report-childasin",
        "gcs_file_prefix": "sp-api-sales-and-traffic-report-childasin-",
        "report_options": {
            "dateGranularity": "DAY",
            "asinGranularity": "CHILD"
        }
    }
]
# ===================================================================

# (get_sp_api_access_token, upload_to_gcs 関数は変更なし)
def get_sp_api_access_token(client_id, client_secret, refresh_token):
    # ... (変更なし)
    print("-> SP-APIアクセストークンを取得中...")
    try:
        if not all([client_id, client_secret, refresh_token]):
            raise ValueError("シークレットが環境変数から正しく取得できませんでした。")
        response = requests.post("https://api.amazon.com/auth/o2/token", headers={"Content-Type": "application/x-www-form-urlencoded"}, data={"grant_type": "refresh_token", "refresh_token": refresh_token, "client_id": client_id, "client_secret": client_secret})
        response.raise_for_status()
        access_token = response.json().get("access_token")
        print("-> SP-APIアクセストークンの取得に成功しました。")
        return access_token
    except Exception as e:
        print(f"Error: SP-APIアクセストークンの取得に失敗しました: {e}")
        raise

def upload_to_gcs(bucket_name, blob_name, content):
    # ... (変更なし)
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content, content_type='application/json')
        print(f"  -> GCSへの保存成功: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f"  -> Error: GCSへのアップロードに失敗しました: {e}")
        pass

def main(request):
    print("--- 処理開始 ---")
    try:
        client_id = os.environ.get("SP_API_CLIENT_ID")
        client_secret = os.environ.get("SP_API_CLIENT_SECRET")
        refresh_token = os.environ.get("SP_API_REFRESH_TOKEN")

        access_token = get_sp_api_access_token(client_id, client_secret, refresh_token)
        headers = {'Content-Type': 'application/json', 'x-amz-access-token': access_token}

        utc_now = datetime.now(timezone.utc)
        start_date = utc_now - timedelta(days=START_DAYS_AGO)
        end_date = utc_now - timedelta(days=END_DAYS_AGO)
        print(f"データ取得期間: {start_date.strftime('%Y-%m-%d')} から {end_date.strftime('%Y-%m-%d')}")

        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\n[{date_str}] の処理を開始...")

            for config in REPORT_CONFIGS:
                print(f"  -> レポート種別: [{config['type']}] の処理を開始...")
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
                    
                    response = requests.post(f"{SP_API_ENDPOINT}/reports/2021-06-30/reports", headers=headers, data=payload)
                    response.raise_for_status()
                    report_id = response.json()["reportId"]
                    print(f"    -> レポート作成リクエスト成功 (Report ID: {report_id})")
                    
                    # (ポーリング、ダウンロード、アップロードのロジックはNotebook版と同様)
                    get_report_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}"
                    report_document_id = None
                    for attempt in range(15):
                        time.sleep(20)
                        response = requests.get(get_report_url, headers=headers)
                        response.raise_for_status()
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

                    get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
                    response = requests.get(get_doc_url, headers=headers)
                    response.raise_for_status()
                    download_url = response.json()["url"]
                    response = requests.get(download_url)
                    response.raise_for_status()
                    with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                        report_content = f.read()
                    print(f"    -> レポートのダウンロードと解凍が完了。")

                    if report_content.strip():
                        blob_name = f"{config['gcs_file_prefix']}{current_date.strftime('%Y%m%d')}.json"
                        upload_to_gcs(config['gcs_bucket_name'], blob_name, report_content)
                    else:
                        print("    -> レポート内容が空のためスキップ。")

                except Exception as e:
                    print(f"    -> Error: [{config['type']}] の処理中にエラー発生: {e}")
                    continue
                finally:
                    time.sleep(2)

            current_date += timedelta(days=1)

    except Exception as e:
        print(f"致命的なエラーが発生しました: {e}")
        return ("Internal Server Error", 500)
    
    print("\n--- 全体処理完了 ---")
    return ("OK", 200)