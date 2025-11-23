"""
Ledger Summary View Data Report Module

このモジュールは、SP-APIのGET_LEDGER_SUMMARY_VIEW_DATAレポートを取得し、GCSに保存します。
- 取得期間: 前月（月初から月末まで）
- 頻度: 毎日実行（前月のデータを上書き）
- 保存形式: TSV (Tab-Separated Values)
"""

import json
import time
import gzip
import io
import requests
import calendar
from datetime import datetime, timedelta, timezone
from google.cloud import storage
from utils.sp_api_auth import get_access_token
from utils.http_retry import request_with_retry


# ===================================================================
# 設定
# ===================================================================
MARKETPLACE_ID = "A1VC38T7YXB528"  # 日本
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
GCS_BUCKET_NAME = "sp-api-ledger-summary-view-data"
GCS_FILE_PREFIX = "sp-api-ledger-summary-view-data-"


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


def _get_previous_month_range():
    """
    先月の初日と末日を計算します。
    
    Returns:
        tuple: (start_date, end_date) datetime objects
        start_date: 先月の1日 00:00:00
        end_date: 先月の末日 00:00:00 (時刻は呼び出し元で調整可能だが、datetimeオブジェクトとしては00:00:00)
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
    Ledger Summary View Data Reportの取得とGCS保存を実行します。
    """
    print("\n=== Ledger Summary View Data Report 処理開始 ===")
    
    try:
        # アクセストークン取得
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': access_token
        }
        
        # データ取得期間を計算 (前月)
        start_date, end_date = _get_previous_month_range()
        
        # 文字列形式に変換 (YYYY-MM-DDT00:00:00Z)
        # dataStartTime: 月初 00:00:00Z
        # dataEndTime: 月末 00:00:00Z (ドキュメント/サンプルに基づく)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        print(f"データ取得期間: {start_date_str} から {end_date_str} (MONTHLY集計)")
        
        try:
            # レポート作成リクエスト
            payload_dict = {
                "marketplaceIds": [MARKETPLACE_ID],
                "reportType": "GET_LEDGER_SUMMARY_VIEW_DATA",
                "dataStartTime": f"{start_date_str}T00:00:00Z",
                "dataEndTime": f"{end_date_str}T00:00:00Z",
                "reportOptions": {
                    "aggregatedByTimePeriod": "MONTHLY"
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
                print(f"    -> Warn: レポート処理がタイムアウトしました。スキップします。")
                return
            
            # レポートドキュメントのダウンロードURL取得
            get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
            response = request_with_retry('GET', get_doc_url, headers=headers)
            download_url = response.json()["url"]
            
            # レポートをダウンロードして解凍
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
                # ファイル名: sp-api-ledger-summary-view-data-yyyymm.tsv
                blob_name = f"{GCS_FILE_PREFIX}{start_date.strftime('%Y%m')}.tsv"
                _upload_to_gcs(GCS_BUCKET_NAME, blob_name, report_content)
            else:
                print("    -> レポート内容が空のためスキップ。")
        
        except Exception as e:
            print(f"    -> Error: レポート処理中にエラー発生: {e}")
            raise

        print("\n=== Ledger Summary View Data Report 処理完了 ===")

    except Exception as e:
        print(f"Error: Ledger Summary View Data Report処理中に致命的なエラーが発生しました: {e}")
        raise
