"""
Ledger Detail View Data Report Module

このモジュールは、SP-APIのGET_LEDGER_DETAIL_VIEW_DATAレポートを取得し、GCSに保存します。
- 取得期間: 8日前から1日前までの各日
- 頻度: 毎日実行（期間が重複する場合は上書き）
- 保存形式: TSV (Tab-Separated Values)
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
GCS_BUCKET_NAME = "sp-api-ledger-detail-view-data"
GCS_FILE_PREFIX = "sp-api-ledger-detail-view-data-"


def _upload_to_gcs(bucket_name, blob_name, content):
    """
    GCSにファイルをアップロードします。
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        # TSVファイルとして保存 (text/tab-separated-values)
        blob.upload_from_string(content, content_type='text/tab-separated-values')
        print(f"  -> GCSへの保存成功: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f"  -> Error: GCSへのアップロードに失敗しました: {e}")


def run():
    """
    Ledger Detail View Data Reportの取得とGCS保存を実行します。
    """
    print("\n=== Ledger Detail View Data Report 処理開始 ===")
    
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
                # dataStartTime: その日の00:00:00Z
                # dataEndTime: その日の23:59:59Z (または翌日00:00:00Z)
                # サンプルでは翌日00:00:00Zを使用しているようなのでそれに合わせる
                next_date = current_date + timedelta(days=1)
                next_date_str = next_date.strftime('%Y-%m-%d')
                
                payload_dict = {
                    "marketplaceIds": [MARKETPLACE_ID],
                    "reportType": "GET_LEDGER_DETAIL_VIEW_DATA",
                    "dataStartTime": f"{date_str}T00:00:00Z",
                    "dataEndTime": f"{next_date_str}T00:00:00Z"
                }
                
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
                # Ledger Detail View DataはTSV形式で返されることが多い
                response = request_with_retry('GET', download_url)
                
                # gzip圧縮されているか確認して解凍
                try:
                    with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                        report_content = f.read()
                except (OSError, UnicodeDecodeError):
                    # gzipでない、またはUTF-8でデコードできない場合
                    # CP932(Shift-JIS)で試行
                    try:
                        if response.content[:2] == b'\x1f\x8b': # GZIP magic number check
                             with gzip.open(io.BytesIO(response.content), 'rt', encoding='cp932') as f:
                                report_content = f.read()
                        else:
                            report_content = response.content.decode('cp932')
                    except UnicodeDecodeError:
                         # それでもだめなら ISO-8859-1 (latin-1)
                        if response.content[:2] == b'\x1f\x8b':
                             with gzip.open(io.BytesIO(response.content), 'rt', encoding='iso-8859-1') as f:
                                report_content = f.read()
                        else:
                            report_content = response.content.decode('iso-8859-1')
                
                print(f"    -> レポートのダウンロード完了。")
                
                # GCSに保存
                if report_content.strip():
                    # ファイル名: prefix-YYYYMMDD-YYYYMMDD.tsv
                    # 1日分なので start-start (同日) とする
                    blob_name = f"{GCS_FILE_PREFIX}{current_date.strftime('%Y%m%d')}-{current_date.strftime('%Y%m%d')}.tsv"
                    _upload_to_gcs(GCS_BUCKET_NAME, blob_name, report_content)
                else:
                    print("    -> レポート内容が空のためスキップ。")
            
            except Exception as e:
                print(f"    -> Error: {date_str} の処理中にエラー発生: {e}")
                # エラーが発生しても次の日付に進むため、continueはしない（またはfinallyでインクリメントする）
            
            finally:
                time.sleep(2)
            
            # 次の日付へ
            current_date += timedelta(days=1)
            
        print("\n=== Ledger Detail View Data Report 処理完了 ===")

    except Exception as e:
        print(f"Error: Ledger Detail View Data Report処理中に致命的なエラーが発生しました: {e}")
        raise
