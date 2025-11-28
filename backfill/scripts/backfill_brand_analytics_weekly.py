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


# バックフィル期間（過去2年）
BACKFILL_YEARS = 2


def _fetch_inventory_summaries(access_token, next_token=None):
    """FBA Inventory APIから在庫サマリーを取得します。"""
    url = f"{SP_API_ENDPOINT}/fba/inventory/v1/summaries"
    
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    params = {
        "marketplaceIds": MARKETPLACE_ID,
        "granularityType": "Marketplace",
        "granularityId": MARKETPLACE_ID
    }
    
    if next_token:
        params["nextToken"] = next_token
    
    response = request_with_retry("GET", url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        error_msg = f"FBA Inventory API Error: {response.status_code} - {response.text}"
        print(f"  -> Error: {error_msg}")
        raise Exception(error_msg)


def get_asin_list_from_api(access_token):
    """FBA在庫からASINリストを取得します。"""
    all_asins = set()
    next_token = None
    page = 1
    
    while True:
        response_data = _fetch_inventory_summaries(access_token, next_token)
        
        # Handle payload wrapper if present
        if "payload" in response_data:
            summaries = response_data["payload"].get("inventorySummaries", [])
        else:
            summaries = response_data.get("inventorySummaries", [])
            
        for summary in summaries:
            asin = summary.get("asin")
            if asin:
                all_asins.add(asin)
        
        # 次のページがあるかチェック
        pagination = response_data.get("pagination", {})
        next_token = pagination.get("nextToken")
        
        if not next_token:
            break
            
        page += 1
    
    return sorted(list(all_asins))

def get_week_range(end_date):
    """
    指定された終了日から1週間の範囲を計算します（日曜日〜土曜日）。
    
    Args:
        end_date: 終了日（土曜日）
        
    Returns:
        tuple: (start_date, end_date)
    """
    start_date = end_date - timedelta(days=6)
    # Postmanに合わせて土曜日の日付をそのまま使う
    return start_date, end_date



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




def fetch_report(period, start_date, end_date, headers, asin_list, max_attempts=20, retry_delay=15):
    """
    レポートを取得します。ASINリストを分割してリクエストし、結果を統合します。
    
    Args:
        period: "WEEK" または "MONTH"
        start_date: 開始日
        end_date: 終了日
        headers: HTTPヘッダー
        asin_list: 対象ASINのリスト
        
    Returns:
        tuple: (content, is_fatal, is_timeout)
            content (str): レポート内容（NDJSON形式）、またはNone
            is_fatal (bool): FATALエラーかどうか
            is_timeout (bool): タイムアウトかどうか
    """
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    all_ndjson_lines = []
    chunk_size = 10
    
    print(f"      ASIN数: {len(asin_list)} (10件ずつ {len(asin_list)//chunk_size + 1}回に分割して取得)")
    
    for i in range(0, len(asin_list), chunk_size):
        chunk = asin_list[i:i + chunk_size]
        asin_str = " ".join(chunk)
        
        # レポート作成リクエスト
        payload_dict = {
            "marketplaceIds": [MARKETPLACE_ID],
            "reportType": "GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT",
            "dataStartTime": f"{start_date_str}T00:00:00.000Z",
            "dataEndTime": f"{end_date_str}T00:00:00.000Z",
            "reportOptions": {
                "reportPeriod": period,
                "asin": asin_str
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
                    print(f"        [Chunk {i//chunk_size + 1}] ...処理中 (Status: {status}, {attempt+1}/{max_attempts})")
                
                if status == "DONE":
                    report_document_id = response.json()["reportDocumentId"]
                    break
                elif status in ["FATAL", "CANCELLED"]:
                    print(f"        [Chunk {i//chunk_size + 1}] レポート処理失敗 (Status: {status})")
                    # FATALの場合はこのチャンクをスキップして次へ
                    break
            
            if not report_document_id:
                print(f"        [Chunk {i//chunk_size + 1}] タイムアウトまたは失敗")
                continue
            
            # レポートダウンロード
            get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
            response = request_with_retry('GET', get_doc_url, headers=headers)
            download_url = response.json()["url"]
            
            response = request_with_retry('GET', download_url)
            with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                report_content = f.read()
            
            # NDJSON形式に変換してリストに追加
            json_data = json.loads(report_content)
            items = json_data.get("dataByAsin", [])
            
            if items:
                chunk_lines = [json.dumps(item, ensure_ascii=False) for item in items]
                all_ndjson_lines.extend(chunk_lines)
                print(f"        [Chunk {i//chunk_size + 1}] {len(items)}件取得")
            else:
                print(f"        [Chunk {i//chunk_size + 1}] データなし")
        
        except Exception as e:
            print(f"        [Chunk {i//chunk_size + 1}] エラー: {e}")
            continue

    if all_ndjson_lines:
        return "\n".join(all_ndjson_lines), False, False
    else:
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
    
    # データ確定待ちのため、さらに1週間遡る
    latest_saturday -= timedelta(days=7)
    
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
    
    # ASINリストを取得
    print("  -> FBA InventoryからASINリストを取得中...")
    try:
        asin_list = get_asin_list_from_api(access_token)
        print(f"  -> 取得ASIN数: {len(asin_list)}")
    except Exception as e:
        print(f"  -> Error: ASINリストの取得に失敗しました: {e}")
        return

    for start_date, end_date in get_all_week_ranges(latest_saturday):
        # ファイル名にはAPIに渡す実際のend_dateを使用する
        filename = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}.json"
        filepath = week_dir / filename
        
        if filepath.exists():
            print(f"  [SKIP] {filename} (既存)")
            skip_count += 1
            continue
        
        print(f"  [取得中] {filename}")
        # 週次は早めに諦める (10回 x 10秒 = 100秒)
        content, is_fatal, is_timeout = fetch_report("WEEK", start_date, end_date, headers, asin_list, max_attempts=10, retry_delay=10)
        
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
