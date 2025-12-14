"""
Brand Analytics Repeat Purchase Report (MONTHLY) - Historical Data Backfill

このスクリプトは、過去2年分のBrand Analytics Repeat Purchase Report（月次）を取得します。
取得したデータは backfill/data/brand-analytics-repeat-purchase/monthly/ に保存されます。
"""

import json
import time
import gzip
import io
import sys
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
REPORT_TYPE = "GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT"
DATA_DIR = Path(__file__).parent.parent / "data" / "brand-analytics-repeat-purchase"

# バックフィル期間（過去2年）
BACKFILL_YEARS = 2


def get_all_month_ranges(start_from_date):
    """
    指定された日付から過去に遡って、すべての月範囲を生成します。
    """
    current_date = start_from_date.replace(day=1)
    cutoff_date = start_from_date - timedelta(days=365 * BACKFILL_YEARS)
    
    while current_date >= cutoff_date:
        # 月の最終日を計算
        if current_date.month == 12:
            next_month = current_date.replace(year=current_date.year + 1, month=1)
        else:
            next_month = current_date.replace(month=current_date.month + 1)
        
        # end_dateは月の最終日（翌月の初日 - 1日）
        last_day_of_month = next_month - timedelta(days=1)
        
        yield (current_date, last_day_of_month)
        
        # 前月へ
        current_date = (current_date - timedelta(days=1)).replace(day=1)


def fetch_report(start_date, end_date, headers, max_attempts=60, retry_delay=15):
    """
    レポートを取得します。
    """
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # レポート作成リクエスト
    payload_dict = {
        "marketplaceIds": [MARKETPLACE_ID],
        "reportType": REPORT_TYPE,
        "dataStartTime": start_date_str,
        "dataEndTime": end_date_str,
        "reportOptions": {
            "reportPeriod": "MONTH"
        }
    }
    
    payload = json.dumps(payload_dict)
    
    try:
        # レポート作成
        response = request_with_retry(
            'POST',
            f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
            headers=headers,
            data=payload,
            max_retries=5,
            retry_delay=50
        )
        report_id = response.json()["reportId"]
        
        # レポート完了を待機
        get_report_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}"
        report_document_id = None
        
        for attempt in range(max_attempts):
            time.sleep(retry_delay)
            response = request_with_retry(
                'GET',
                get_report_url,
                headers=headers,
                max_retries=3
            )
            status = response.json().get("processingStatus")
            
            if attempt % 5 == 0:
                print(f"        ...処理中 (Status: {status}, {attempt+1}/{max_attempts})")
            
            if status == "DONE":
                report_document_id = response.json()["reportDocumentId"]
                break
            elif status in ["FATAL", "CANCELLED"]:
                print(f"        レポート処理失敗 (Status: {status})")
                if status == "FATAL":
                    return None, True, False
                return None, False, False
        
        if not report_document_id:
            print(f"        タイムアウト")
            return None, False, True
        
        # レポートダウンロード
        get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
        response = request_with_retry('GET', get_doc_url, headers=headers)
        download_url = response.json()["url"]
        
        response = request_with_retry('GET', download_url)
        
        # 圧縮されている可能性があるため展開
        try:
            with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                report_content = f.read()
        except gzip.BadGzipFile:
            report_content = response.content.decode('utf-8')
            
        return report_content, False, False
    
    except Exception as e:
        print(f"        エラー: {e}")
        return None, False, False


def backfill_monthly():
    """月次データのバックフィルを実行します。"""
    print("\n=== Brand Analytics Repeat Purchase (MONTHLY) バックフィル開始 ===")
    
    utc_now = datetime.now(timezone.utc)
    # 先月の末日
    this_month_first = utc_now.replace(day=1)
    last_month_end = this_month_first - timedelta(days=1)
    
    access_token = get_access_token()
    headers = {
        'Content-Type': 'application/json',
        'x-amz-access-token': access_token
    }
    
    month_dir = DATA_DIR / "monthly"
    month_dir.mkdir(parents=True, exist_ok=True)
    
    success_count = 0
    skip_count = 0
    consecutive_errors = 0
    consecutive_timeouts = 0
    max_consecutive_errors = 5
    max_consecutive_timeouts = 3
    
    for start_date, end_date in get_all_month_ranges(last_month_end):
        filename = f"{start_date.strftime('%Y%m')}.json"
        filepath = month_dir / filename
        
        if filepath.exists():
            print(f"  [SKIP] {filename} (既存)")
            skip_count += 1
            continue
        
        print(f"  [取得中] {filename} ({start_date.date()} - {end_date.date()})")
        
        content, is_fatal, is_timeout = fetch_report(start_date, end_date, headers)
        
        if content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"    ✓ 保存完了")
            success_count += 1
            consecutive_errors = 0
            consecutive_timeouts = 0
        else:
            skip_count += 1
            consecutive_errors += 1
            if is_timeout:
                consecutive_timeouts += 1
            else:
                consecutive_timeouts = 0
            
            # 連続エラー時は中断
            if is_fatal:
                print(f"      FATALエラーが発生したため、中断します。")
                break

            if consecutive_timeouts >= max_consecutive_timeouts:
                print(f"      連続してタイムアウトが発生したため中断します。")
                break
                
            if consecutive_errors >= max_consecutive_errors:
                print(f"      連続エラーが{max_consecutive_errors}回発生しました。中断します。")
                break
            
            time.sleep(10)
            continue
        
        time.sleep(15)  # レート制限対策
    
    print(f"\n完了: 成功 {success_count}件, スキップ {skip_count}件")


if __name__ == "__main__":
    try:
        backfill_monthly()
    except KeyboardInterrupt:
        print("\n中断されました")
    except Exception as e:
        print(f"\nエラー: {e}")
        import traceback
        traceback.print_exc()
