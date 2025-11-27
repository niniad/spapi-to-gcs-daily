"""
Brand Analytics Search Query Performance Report (WEEKLY) - Historical Data Backfill

このスクリプトは、過去2年分のBrand Analyticsレポート（週次）を取得します。
取得したデータは backfill/data/brand-analytics/WEEK/ に保存されます。
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
DATA_DIR = Path(__file__).parent.parent / "data" / "brand-analytics"

# 対象ASINリスト
ASIN_LIST = [
    "B0D894LS44", "B0D89H2L67", "B0D89DTD29", "B0D88XNCHG", "B0DBSM5ZDZ",
    "B0DBSF1CZ6", "B0DBS2WWJN", "B0DBS1ZQ7K", "B0DBS2CK1T", "B0DBSB6XY9",
    "B0DT5P24N2", "B0DT51B33M", "B0FRZ3Z755", "B0FRZ2D3G2"
]

# バックフィル期間（過去2年）
BACKFILL_YEARS = 2


def get_week_range(end_date):
    """
    指定された終了日から1週間の範囲を計算します（日曜日〜土曜日）。
    
    Args:
        end_date: 終了日（土曜日）
        
    Returns:
        tuple: (start_date, end_date)
    """
    start_date = end_date - timedelta(days=6)
    # end_dateを翌日の00:00:00にする（土曜日のデータを含めるため）
    end_date_next_day = end_date + timedelta(days=1)
    return start_date, end_date_next_day


def get_all_week_ranges(start_from_date):
    """
    指定された日付から過去に遡って、すべての週範囲を生成します。
    
    Args:
        start_from_date: 開始日（最新の土曜日）
        
    Yields:
        tuple: (start_date, end_date) の週範囲
    """
    current_end = start_from_date
    cutoff_date = start_from_date - timedelta(days=365 * BACKFILL_YEARS)
    
    while current_end >= cutoff_date:
        start, end = get_week_range(current_end)
        if start >= cutoff_date:
            yield (start, end)
        current_end -= timedelta(days=7)


def fetch_report(period, start_date, end_date, headers, max_attempts=20, retry_delay=15):
    """
    レポートを取得します。
    
    Args:
        period: "WEEK" または "MONTH"
        start_date: 開始日
        end_date: 終了日
        headers: HTTPヘッダー
        
    Returns:
        tuple: (content, is_fatal, is_timeout)
            content (str): レポート内容（NDJSON形式）、またはNone
            is_fatal (bool): FATALエラーかどうか
            is_timeout (bool): タイムアウトかどうか
    """
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # レポート作成リクエスト
    payload_dict = {
        "marketplaceIds": [MARKETPLACE_ID],
        "reportType": "GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT",
        "dataStartTime": f"{start_date_str}T00:00:00Z",
        "dataEndTime": f"{end_date_str}T00:00:00Z",
        "reportOptions": {
            "reportPeriod": period,
            "asin": " ".join(ASIN_LIST)
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
                print(f"      ...処理中 (Status: {status}, {attempt+1}/{max_attempts})")
            
            if status == "DONE":
                report_document_id = response.json()["reportDocumentId"]
                break
            elif status in ["FATAL", "CANCELLED"]:
                print(f"      レポート処理失敗 (Status: {status})")
                return None, True, False
        
        if not report_document_id:
            print(f"      タイムアウト ({max_attempts}回試行しても完了しませんでした)")
            return None, False, True
        
        # レポートダウンロード
        get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
        response = request_with_retry('GET', get_doc_url, headers=headers)
        download_url = response.json()["url"]
        
        response = request_with_retry('GET', download_url)
        with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
            report_content = f.read()
        
        # NDJSON形式に変換
        json_data = json.loads(report_content)
        items = json_data.get("dataByAsin", [])
        
        if items:
            ndjson_lines = [json.dumps(item, ensure_ascii=False) for item in items]
            return "\\n".join(ndjson_lines), False, False
        else:
            print(f"      データなし")
            return None, False, False
    
    except Exception as e:
        print(f"      エラー: {e}")
        return None, False, False


def backfill_weekly():
    """週次データのバックフィルを実行します。"""
    print("\\n=== 週次データのバックフィル開始 ===")
    
    # 最新の土曜日を計算
    utc_now = datetime.now(timezone.utc)
    weekday = utc_now.weekday()
    days_since_saturday = (weekday - 5) % 7
    if days_since_saturday == 0:
        days_since_saturday = 7
    latest_saturday = utc_now - timedelta(days=days_since_saturday)
    
    access_token = get_access_token()
    headers = {
        'Content-Type': 'application/json',
        'x-amz-access-token': access_token
    }
    
    week_dir = DATA_DIR / "WEEK"
    week_dir.mkdir(parents=True, exist_ok=True)
    
    success_count = 0
    skip_count = 0
    consecutive_errors = 0
    consecutive_timeouts = 0
    max_consecutive_errors = 5
    max_consecutive_timeouts = 3
    
    for start_date, end_date in get_all_week_ranges(latest_saturday):
        # ファイル名は土曜日（end_date - 1日）の日付を使用する
        filename_end_date = end_date - timedelta(days=1)
        filename = f"{start_date.strftime('%Y%m%d')}-{filename_end_date.strftime('%Y%m%d')}.json"
        filepath = week_dir / filename
        
        if filepath.exists():
            print(f"  [SKIP] {filename} (既存)")
            skip_count += 1
            continue
        
        print(f"  [取得中] {filename}")
        # 週次は早めに諦める (10回 x 10秒 = 100秒)
        content, is_fatal, is_timeout = fetch_report("WEEK", start_date, end_date, headers, max_attempts=10, retry_delay=10)
        
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
            
            # 連続エラー時は待機時間を増やす
            if is_fatal:
                print(f"      FATALエラーが発生したため、これ以上過去のデータは取得できません。")
                break
            
            if consecutive_timeouts >= max_consecutive_timeouts:
                print(f"      連続してタイムアウトが発生したため({max_consecutive_timeouts}回)、データ保持期限切れと判断して停止します。")
                break
            
            if consecutive_errors >= max_consecutive_errors:
                print(f"\\n  連続エラーが{max_consecutive_errors}回発生しました。処理を中断します。")
                break
            elif consecutive_errors >= 3:
                wait_time = min(30, consecutive_errors * 5)
                print(f"  {wait_time}秒待機します...")
                time.sleep(wait_time)
                continue
        
        time.sleep(50)  # レート制限対策（理論値45秒+バッファ）
    
    print(f"\\n週次データ完了: 成功 {success_count}件, スキップ {skip_count}件")


if __name__ == "__main__":
    print("Brand Analytics (WEEKLY) - Historical Data Backfill")
    print("=" * 60)
    
    try:
        backfill_weekly()
        
        print("\\n" + "=" * 60)
        print("バックフィル完了")
        
    except KeyboardInterrupt:
        print("\\n\\n中断されました")
    except Exception as e:
        print(f"\\nエラー: {e}")
        import traceback
        traceback.print_exc()
