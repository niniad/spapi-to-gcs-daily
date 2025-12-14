"""
All Orders Report Module

このモジュールは、SP-APIのAll Orders Report (GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL) を取得し、GCSに保存します。
過去1週間分のデータを日次で取得し、GCS上のファイルを上書き更新します。
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
        print(f"  -> GCSへの保存成功: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f"  -> Error: GCSへのアップロードに失敗しました: {e}")


def run():
    """
    All Orders Reportの取得とGCS保存を実行します。
    
    処理フロー:
    1. SP-APIアクセストークンを取得
    2. 指定期間（過去1週間）の各日付について:
       - レポートを作成・取得
       - GCSに保存 (同名ファイルは上書き)
    """
    print("\n=== All Orders Report 処理開始 ===")
    
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
                print(f"  -> レポート作成リクエスト成功 (Report ID: {report_id})")
                
                # レポート完了を待機(ポーリング)
                get_report_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}"
                report_document_id = None
                
                for attempt in range(20):  # 最大20回試行(約6-7分)
                    time.sleep(20)  # 20秒待機
                    response = request_with_retry(
                        'GET',
                        get_report_url,
                        headers=headers
                    )
                    status = response.json().get("processingStatus")
                    
                    if status == "DONE":
                        report_document_id = response.json()["reportDocumentId"]
                        print(f"  -> レポート作成完了 (DONE)")
                        break
                    elif status in ["FATAL", "CANCELLED"]:
                        print(f"  -> Warn: レポート処理が失敗またはキャンセル (Status: {status})")
                        break
                    else:
                        print(f"  -> レポート作成中 (Status: {status})...")
                
                if not report_document_id:
                    print(f"  -> Warn: レポート処理がタイムアウトしました。スキップします。")
                    current_date += timedelta(days=1)
                    continue
                
                # レポートドキュメントのダウンロードURL取得
                get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
                response = request_with_retry('GET', get_doc_url, headers=headers)
                download_url = response.json()["url"]
                
                # レポートをダウンロードして解凍 (Orders Reportは通常Content-Encodingなしのテキストだが、念のため圧縮対応)
                response = request_with_retry('GET', download_url)
                
                content_to_save = None
                
                # 圧縮されているかチェック（ヘッダー等はAPI仕様によるが、バイナリ判定等で簡易チェックあるいはtry-except）
                # SP-APIのレポートは明示しない限り平文の場合が多いが、gzipで来ることもある。
                try:
                    with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                        content_to_save = f.read()
                    print(f"  -> GZIP解凍完了")
                except gzip.BadGzipFile:
                    # Gzipでない場合はそのままデコード (CP932/Shift-JISの可能性もあるが、SP-API V2系は通常UTF-8)
                    # 日本のレポートはShift_JIS(CP932)で返る場合があるため、decodeを試みる
                    try:
                        content_to_save = response.content.decode('utf-8')
                    except UnicodeDecodeError:
                        try:
                            content_to_save = response.content.decode('cp932')
                            print("  -> Shift_JIS(cp932)でデコードしました")
                        except UnicodeDecodeError:
                             print("  -> Warn: デコードに失敗しました。Latin-1で試行します。")
                             content_to_save = response.content.decode('latin-1')

                print(f"  -> ダウンロード完了。")
                
                # GCSに保存(内容が空でない場合のみ)
                if content_to_save and content_to_save.strip():
                    # ファイル名は YYYYMMDD.tsv とする
                    blob_name = f"{GCS_FILE_PREFIX}{current_date.strftime('%Y%m%d')}.tsv"
                    _upload_to_gcs(GCS_BUCKET_NAME, blob_name, content_to_save)
                else:
                    print("  -> レポート内容が空のためスキップ。")
            
            except Exception as e:
                print(f"  -> Error: [{date_str}] の処理中にエラー発生: {e}")
                # 個別の日付のエラーはログに出して続行
            
            finally:
                time.sleep(2)  # 次のリクエストまで少し待機
            
            # 次の日付へ
            current_date += timedelta(days=1)
        
        print("\n=== All Orders Report 処理完了 ===")
        
    except Exception as e:
        print(f"Error: All Orders Report処理中に致命的なエラーが発生しました: {e}")
        raise

if __name__ == "__main__":
    run()
