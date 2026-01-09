"""
Sales and Traffic Report Backfill Script

このスクリプトは、Sales and Traffic Report (GET_SALES_AND_TRAFFIC_REPORT) を使用して
過去の売上・トラフィックデータを取得し、JSON形式で保存します。
- Day granularity (Overall Sales/Traffic)
- Child ASIN granularity (Per-ASIN Sales/Traffic)

取得したデータは以下に保存されます:
- backfill/data/sales-and-traffic-report/day/
- backfill/data/sales-and-traffic-report/child-asin/
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
REPORT_TYPE = "GET_SALES_AND_TRAFFIC_REPORT"
DATA_DIR = Path(__file__).parent.parent / "data" / "sales-and-traffic-report"

# Backfill period (past 2 years)
BACKFILL_YEARS = 2

# Report Configuration
REPORT_CONFIGS = [
    {
        "type": "DAY",
        "subdir": "day",
        "report_options": {}
    },
    {
        "type": "CHILD_ASIN",
        "subdir": "child-asin",
        "report_options": {
            "dateGranularity": "DAY",
            "asinGranularity": "CHILD"
        }
    }
]

def fetch_report_for_date(date_str, report_options, access_token):
    """
    指定された日付のSales and Traffic Reportを作成・ダウンロードします。
    """
    headers = {
        'Content-Type': 'application/json',
        'x-amz-access-token': access_token
    }
    
    # 1. Create Report Request
    payload_dict = {
        "marketplaceIds": [MARKETPLACE_ID],
        "reportType": REPORT_TYPE,
        "dataStartTime": f"{date_str}T00:00:00Z",
        "dataEndTime": f"{date_str}T23:59:59Z",
    }
    if report_options:
        payload_dict["reportOptions"] = report_options
        
    try:
        response = request_with_retry(
            'POST',
            f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
            headers=headers,
            data=json.dumps(payload_dict)
        )
        report_id = response.json()["reportId"]
    except Exception as e:
        print(f"      ! Error: レポート作成リクエスト失敗: {e}")
        raise e

    # 2. Wait for report completion
    get_report_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}"
    report_document_id = None
    
    for attempt in range(20):  # Max 20 attempts (approx 7 mins)
        time.sleep(20)
        try:
            response = request_with_retry('GET', get_report_url, headers=headers)
            status = response.json().get("processingStatus")
            
            if status == "DONE":
                report_document_id = response.json().get("reportDocumentId")
                break
            elif status in ["FATAL", "CANCELLED"]:
                print(f"      ! Error: レポート処理失敗 (Status: {status})")
                return None
            
            if attempt > 0 and attempt % 3 == 0:
                 print(f"      ...処理中 ({attempt}回目)")
                 
        except Exception as e:
             print(f"      ! Warning: ステータス確認失敗 ({e}) - リトライします")

    if not report_document_id:
        print(f"      ! Error: タイムアウトしました")
        return None

    # 3. Get Download URL
    get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
    response = request_with_retry('GET', get_doc_url, headers=headers)
    download_url = response.json()["url"]
    
    # 4. Download and Decompress
    response = request_with_retry('GET', download_url)
    
    content = None
    try:
        with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
            content = f.read()
    except gzip.BadGzipFile:
        content = response.content.decode('utf-8')

    return content

def backfill_sales_and_traffic():
    """Sales and Traffic Reportのバックフィルを実行します。"""
    print("\n=== Sales and Traffic Report バックフィル開始 ===")
    
    utc_now = datetime.now(timezone.utc)
    start_date = utc_now - timedelta(days=1)
    cutoff_date = utc_now - timedelta(days=365 * BACKFILL_YEARS)
    
    # Create target directories
    for config in REPORT_CONFIGS:
        (DATA_DIR / config["subdir"]).mkdir(parents=True, exist_ok=True)
    
    current_date = start_date
    access_token = get_access_token()
    
    while current_date >= cutoff_date:
        date_str = current_date.strftime('%Y%m%d')
        date_iso = current_date.strftime('%Y-%m-%d')
        
        print(f"\n[{date_iso}] の処理中...")
        
        for config in REPORT_CONFIGS:
            subdir = config["subdir"]
            filename = f"{date_str}.json"
            filepath = DATA_DIR / subdir / filename
            
            if filepath.exists():
                print(f"  [{subdir}] [SKIP] {filename} (既存)")
                continue
            
            print(f"  [{subdir}] [取得中] {filename}")
            
            try:
                content = fetch_report_for_date(date_iso, config["report_options"], access_token)
                
                if content and content.strip():
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(content)
                    print(f"    ✓ 保存完了")
                else:
                    print(f"    - データなしまたは空")
                    # Create empty file to skip next time
                    with open(filepath, 'w', encoding='utf-8') as f:
                        pass
                    
            except Exception as e:
                print(f"    ! Error: {e}")
                pass
            
            time.sleep(2) # API throttling buffer
        
        current_date -= timedelta(days=1)

    print("\nSales and Traffic Report バックフィル完了")

if __name__ == "__main__":
    try:
        backfill_sales_and_traffic()
    except KeyboardInterrupt:
        print("\n中断されました")
    except Exception as e:
        print(f"\nエラー: {e}")
        import traceback
        traceback.print_exc()
