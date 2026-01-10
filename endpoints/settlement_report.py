"""
Settlement Report Module

このモジュールは、SP-APIのSettlement Reportを取得し、GCSに保存します。
既存のレポート一覧を取得し、GCSに未保存のレポートのみダウンロードします。
"""

import requests
import gzip
import io
import logging
from datetime import datetime
from google.cloud import storage
from google.cloud.exceptions import NotFound
from utils.sp_api_auth import get_access_token
from utils.http_retry import request_with_retry


# ===================================================================
# 設定
# ===================================================================
MARKETPLACE_ID = "A1VC38T7YXB528"  # 日本
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
REPORT_TYPE = "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2"
GCS_BUCKET_NAME = "sp-api-bucket"
FILE_PREFIX = "settlement-report-data-flat-file-v2/"


def _format_date_for_filename(iso_datetime_str):
    """
    ISO形式の日時文字列をファイル名用の日付形式(YYYYMMDD)に変換します。
    
    Args:
        iso_datetime_str: ISO形式の日時文字列 (例: "2025-11-17T11:06:19+00:00")
        
    Returns:
        str: YYYYMMDD形式の日付文字列 (例: "20251117")
    """
    try:
        # ISO形式をパース
        dt = datetime.fromisoformat(iso_datetime_str.replace('Z', '+00:00'))
        return dt.strftime('%Y%m%d')
    except Exception as e:
        logging.warning(f"日付変換エラー ({iso_datetime_str}): {e}")
        # フォールバック: 最初の10文字(YYYY-MM-DD)からハイフンを削除
        return iso_datetime_str[:10].replace('-', '')


def _generate_filename(report):
    """
    レポート情報からGCSファイル名を生成します。
    
    Args:
        report: レポート情報の辞書
        
    Returns:
        str: GCSファイル名 (例: "settlement-report-data-flat-file-v2-20251103-20251117.tsv")
    """
    start_date = _format_date_for_filename(report['dataStartTime'])
    end_date = _format_date_for_filename(report['dataEndTime'])
    return f"{FILE_PREFIX}{start_date}-{end_date}.tsv"


def _check_file_exists_in_gcs(bucket_name, blob_name):
    """
    GCSに指定されたファイルが存在するかチェックします。
    
    Args:
        bucket_name: GCSバケット名
        blob_name: ファイル名
        
    Returns:
        bool: ファイルが存在する場合True、存在しない場合False
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception as e:
        logging.warning(f"GCS存在チェックエラー ({blob_name}): {e}")
        return False


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
        # TSVファイルとして保存
        blob.upload_from_string(content, content_type='text/tab-separated-values')
        logging.info(f"GCSへの保存成功: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        logging.error(f"GCSへのアップロードに失敗しました: {e}")


def run():
    """
    Settlement Reportの取得とGCS保存を実行します。
    
    処理フロー:
    1. SP-APIアクセストークンを取得
    2. 既存のSettlement Report一覧を取得
    3. 各レポートについて:
       - GCSに既に保存されているかチェック
       - 未保存の場合のみダウンロードしてGCSに保存
    """
    logging.info("=== Settlement Report 処理開始 ===")
    
    try:
        # アクセストークン取得
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': access_token
        }
        
        # 既存レポート一覧を取得
        logging.info("-> 既存レポート一覧を取得中...")
        params = {
            'reportTypes': REPORT_TYPE,
            'marketplaceIds': MARKETPLACE_ID
        }
        
        response = request_with_retry(
            'GET',
            f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
            headers=headers,
            params=params
        )
        
        reports = response.json().get('reports', [])
        logging.info(f"-> 取得したレポート数: {len(reports)}")
        
        # DONE状態のレポートのみ処理
        done_reports = [r for r in reports if r.get('processingStatus') == 'DONE']
        logging.info(f"-> 処理対象レポート数 (DONE): {len(done_reports)}")
        
        # 各レポートを処理
        downloaded_count = 0
        skipped_count = 0
        
        for report in done_reports:
            report_id = report['reportId']
            report_document_id = report.get('reportDocumentId')
            
            if not report_document_id:
                logging.warning(f"Report ID {report_id} にはreportDocumentIdがありません。スキップします。")
                skipped_count += 1
                continue
            
            # ファイル名を生成
            filename = _generate_filename(report)
            
            # GCSに既に存在するかチェック
            if _check_file_exists_in_gcs(GCS_BUCKET_NAME, filename):
                logging.info(f"スキップ (既存): {filename}")
                skipped_count += 1
                continue
            
            logging.info(f"ダウンロード開始: {filename}")
            
            try:
                # レポートドキュメントのダウンロードURL取得
                get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
                response = request_with_retry('GET', get_doc_url, headers=headers)
                download_url = response.json()["url"]
                
                # レポートをダウンロードして解凍
                response = request_with_retry('GET', download_url)
                
                # gzip形式で圧縮されている場合は解凍
                try:
                    with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                        report_content = f.read()
                except gzip.BadGzipFile:
                    # gzip形式でない場合はそのまま使用
                    report_content = response.text
                
                # GCSに保存
                if report_content.strip():
                    _upload_to_gcs(GCS_BUCKET_NAME, filename, report_content)
                    downloaded_count += 1
                else:
                    logging.warning(f"レポート内容が空です。スキップします。")
                    skipped_count += 1
            
            except Exception as e:
                logging.error(f"Report ID {report_id} の処理中にエラー発生: {e}")
                skipped_count += 1
                continue
        
        logging.info(f"ダウンロード完了: {downloaded_count}件")
        logging.info(f"スキップ: {skipped_count}件")
        logging.info("=== Settlement Report 処理完了 ===")
        
    except Exception as e:
        logging.error(f"Settlement Report処理中に致命的なエラーが発生しました: {e}")
        raise
