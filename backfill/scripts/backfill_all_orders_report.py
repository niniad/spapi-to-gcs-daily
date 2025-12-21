"""
All Orders Report Backfill Script

このスクリプトは、SP-APIのAll Orders Report (GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL) を使用して
過去の注文レポートを取得し、TSV形式で保存します。
取得したデータは backfill/data/all_orders_report/ に保存されます。
"""

import json
import time
import sys
import gzip
import io
from pathlib import Path
from datetime import datetime, timedelta, timezone

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.http_retry import request_with_retry
from backfill.scripts.auth import get_access_token

# ===================================================================
# 設定
# ===================================================================
MARKETPLACE_ID = "A1VC38T7YXB528"  # 日本
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
REPORT_TYPE = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL"
DATA_DIR = Path(__file__).parent.parent / "data" / "all_orders_report"

# バックフィル期間（過去2年）
BACKFILL_YEARS = 2


def fetch_report_for_date(date_str, access_token):
    """
    指定された日付のAll Orders Reportを作成・ダウンロードします。
    """
    headers = {
        'Content-Type': 'application/json',
        'x-amz-access-token': access_token
    }
    
    # 1. レポート作成リクエスト
    # データ期間: 指定日の 00:00:00 〜 23:59:59 (UTC)
    payload_dict = {
        "marketplaceIds": [MARKETPLACE_ID],
        "reportType": REPORT_TYPE,
        "dataStartTime": f"{date_str}T00:00:00Z",
        "dataEndTime": f"{date_str}T23:59:59Z",
    }
    
    print(f"    -> レポート作成リクエスト...")
    try:
        response = request_with_retry(
            'POST',
            f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
            headers=headers,
            data=json.dumps(payload_dict)
        )
        report_id = response.json()["reportId"]
    except Exception as e:
        print(f"    ! Error: レポート作成リクエスト失敗: {e}")
        raise e

    # 2. レポート完了待機
    print(f"    -> 処理待機中 (Report ID: {report_id})...")
    get_report_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}"
    report_document_id = None
    
    for attempt in range(40):  # 最大40回試行(約13分)
        time.sleep(20)
        try:
            response = request_with_retry('GET', get_report_url, headers=headers)
            status = response.json().get("processingStatus")
            
            if status == "DONE":
                report_document_id = response.json()["reportDocumentId"]
                break
            elif status in ["FATAL", "CANCELLED"]:
                print(f"    ! Error: レポート処理失敗 (Status: {status})")
                return None
            else:
                # IN_QUEUE, IN_PROGRESS
                print(f"      - Status: {status}")
        except Exception as e:
             print(f"      ! Warning: ステータス確認失敗 ({e}) - リトライします")

    if not report_document_id:
        print(f"    ! Error: タイムアウトしました")
        return None

    # 3. ダウンロードURL取得
    get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
    response = request_with_retry('GET', get_doc_url, headers=headers)
    download_url = response.json()["url"]
    
    # 4. ダウンロードと解凍
    print(f"    -> ダウンロード中...")
    response = request_with_retry('GET', download_url)
    
    content = None
    try:
        with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
            content = f.read()
    except gzip.BadGzipFile:
        # Gzipでない場合
        try:
            content = response.content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                content = response.content.decode('cp932')
            except UnicodeDecodeError:
                content = response.content.decode('latin-1')

    return content


def backfill_all_orders_report():
    """All Orders Reportのバックフィルを実行します。"""
    print("\n=== All Orders Report バックフィル開始 ===")
    
    utc_now = datetime.now(timezone.utc)
    start_date = utc_now - timedelta(days=1)
    cutoff_date = utc_now - timedelta(days=365 * BACKFILL_YEARS)
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    current_date = start_date
    access_token = get_access_token()
    
    success_count = 0
    skip_count = 0
    
    while current_date >= cutoff_date:
        date_str = current_date.strftime('%Y%m%d')
        date_iso = current_date.strftime('%Y-%m-%d')
        
        filename = f"{date_str}.tsv"
        filepath = DATA_DIR / filename
        
        if filepath.exists():
            print(f"  [SKIP] {filename} (既存)")
            skip_count += 1
            current_date -= timedelta(days=1)
            continue
            
        print(f"  [取得中] {filename} ({date_iso})")
        
        try:
            content = fetch_report_for_date(date_iso, access_token)
            
            if content and content.strip():
                # 改行コードの正規化: 全体のCRLFをLFに統一し、末尾を調整
                final_content = content.replace('\r\n', '\n').strip() + '\n'
                
                # newline='' を指定して、Pythonによる自動変換(\n->\r\n)を無効化
                # これにより、上記で統一した \n がそのまま書き込まれる(または \r\n にしたいなら明示的に書く)
                # ここではUnixスタイル(\n)で保存するか、Windowsスタイル(\r\n)にするか。
                # ユーザーの「改行が多い」という指摘対応のため、LF(\n)のみ、または標準的なCRLFにするが
                # \r\r\n にならないように制御する。
                # Windowsで普通に見れるようにするには \r\n が良いが、二重変換を防ぐため newline='' で書き込むデータ自体を制御する。
                
                # Windows標準の \r\n にしたい場合:
                # final_content = final_content.replace('\n', '\r\n')
                # with open(filepath, 'w', encoding='utf-8', newline='') as f: ...
                
                # シンプルにLF(\n)だけで保存する場合 (多くのツールで無難):
                with open(filepath, 'w', encoding='utf-8', newline='') as f:
                    f.write(final_content)
                print(f"    ✓ 保存完了 (Local)")
                
                # GCSにアップロード
                # ユーザーからのフィードバックによりGCSアップロードは不要とのことなのでコメントアウト
                # エラーの原因調査を優先
                """
                try:
                    from google.cloud import storage
                    storage_client = storage.Client()
                    bucket = storage_client.bucket("sp-api-bucket")
                    blob_name = f"all-orders-report/{filename}"
                    blob = bucket.blob(blob_name)
                    blob.upload_from_filename(str(filepath))
                    print(f"    ✓ GCSアップロード完了: {blob_name}")
                except Exception as e:
                    print(f"    ! Error: GCSアップロード失敗: {e}")
                """

                success_count += 1
            else:
                print(f"    - データなしまたは空")
                # 空ファイルを作成してスキップ用に
                with open(filepath, 'w', encoding='utf-8') as f:
                    pass
                success_count += 1
                
        except Exception as e:
            print(f"    ! Error: {e}")
            pass
        
        current_date -= timedelta(days=1)
        time.sleep(2)

    print(f"\nAll Orders Report バックフィル完了: 成功 {success_count}件, スキップ {skip_count}件")


if __name__ == "__main__":
    try:
        backfill_all_orders_report()
    except KeyboardInterrupt:
        print("\n中断されました")
    except Exception as e:
        print(f"\nエラー: {e}")
        import traceback
        traceback.print_exc()
