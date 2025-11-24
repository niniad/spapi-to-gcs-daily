"""
Ledger Detail View Data Report - Historical Data Backfill

このスクリプトは、過去18ヶ月分のLedger Detailレポートを日次で取得します。
取得したデータは backfill/data/ledger-detail/ に保存されます。
"""

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
DATA_DIR = Path(__file__).parent.parent / "data" / "ledger-detail"

# バックフィル期間（過去18ヶ月）
BACKFILL_MONTHS = 18


def get_all_date_ranges():
    """
    現在日から過去18ヶ月分の日付を生成します。
    
    Yields:
        datetime: 各日の日付
    """
    utc_now = datetime.now(timezone.utc)
    current_date = utc_now - timedelta(days=1)  # 昨日から開始
    cutoff_date = utc_now - timedelta(days=30 * BACKFILL_MONTHS)
    
    while current_date >= cutoff_date:
        yield current_date
        current_date -= timedelta(days=1)


def fetch_report(target_date, headers):
    """
    指定日のレポートを取得します。
    
    Args:
        target_date: 対象日
        headers: HTTPヘッダー
        
    Returns:
        str: レポート内容（TSV形式）、またはNone
    """
    date_str = target_date.strftime('%Y-%m-%d')
    
    # レポート作成リクエスト
    payload = f'''{{
        "reportType": "GET_LEDGER_DETAIL_VIEW_DATA",
        "dataStartTime": "{date_str}T00:00:00Z",
        "dataEndTime": "{date_str}T23:59:59Z",
        "marketplaceIds": ["{MARKETPLACE_ID}"]
    }}'''
    
    try:
        # レポート作成
        response = request_with_retry(
            'POST',
            f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
            headers=headers,
            data=payload,
            max_retries=5,
            retry_delay=60
        )
        report_id = response.json()["reportId"]
        
        # レポート完了を待機
        get_report_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}"
        report_document_id = None
        
        for attempt in range(20):
            time.sleep(15)
            response = request_with_retry(
                'GET',
                get_report_url,
                headers=headers,
                max_retries=5
            )
            status = response.json().get("processingStatus")
            
            if status == "DONE":
                report_document_id = response.json()["reportDocumentId"]
                break
            elif status in ["FATAL", "CANCELLED"]:
                print(f"      レポート処理失敗 (Status: {status})")
                return None
        
        if not report_document_id:
            print(f"      タイムアウト")
            return None
        
        # レポートダウンロード
        get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
        response = request_with_retry('GET', get_doc_url, headers=headers)
        download_url = response.json()["url"]
        
        response = request_with_retry('GET', download_url)
        
        # gzip解凍を試行
        try:
            with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                return f.read()
        except gzip.BadGzipFile:
            # gzipでない場合、複数のエンコーディングを試行
            for encoding in ['utf-8', 'shift-jis', 'cp932']:
                try:
                    return response.content.decode(encoding)
                except UnicodeDecodeError:
                    continue
            # すべて失敗した場合
            print(f"      エラー: エンコーディングの検出に失敗しました")
            return None
        except UnicodeDecodeError:
            # gzipだがutf-8でない場合
            try:
                with gzip.open(io.BytesIO(response.content), 'rt', encoding='shift-jis') as f:
                    return f.read()
            except Exception:
                try:
                    with gzip.open(io.BytesIO(response.content), 'rt', encoding='cp932') as f:
                        return f.read()
                except Exception as e:
                    print(f"      エラー: gzip解凍失敗: {e}")
                    return None
    
    except Exception as e:
        print(f"      エラー: {e}")
        return None


def backfill():
    """Ledger Detailデータのバックフィルを実行します。"""
    consecutive_errors = 0
    max_consecutive_errors = 10
    print("\\n=== Ledger Detail データのバックフィル開始 ===")
    
    access_token = get_access_token()
    headers = {
        'Content-Type': 'application/json',
        'x-amz-access-token': access_token
    }
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    success_count = 0
    skip_count = 0
    total_count = 0
    
    for target_date in get_all_date_ranges():
        total_count += 1
        filename = f"{target_date.strftime('%Y%m%d')}.tsv"
        filepath = DATA_DIR / filename
        
        if filepath.exists():
            if total_count % 50 == 0:
                print(f"  進捗: {total_count}日処理済み (成功: {success_count}, スキップ: {skip_count})")
            skip_count += 1
            continue
        
        print(f"  [{total_count}] {filename}")
        content = fetch_report(target_date, headers)
        
        if content and content.strip():
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"    ✓ 保存完了")
            success_count += 1
            consecutive_errors = 0  # 成功したらリセット
        else:
            print(f"    データなし")
            skip_count += 1
            consecutive_errors += 1
            
            # 連続エラー時は待機時間を増やす
            if consecutive_errors >= max_consecutive_errors:
                print(f"\\n  連続エラーが{max_consecutive_errors}回発生しました。60秒待機します...")
                time.sleep(60)
                consecutive_errors = 0
            elif consecutive_errors >= 5:
                wait_time = min(30, consecutive_errors * 3)
                print(f"  {wait_time}秒待機します...")
                time.sleep(wait_time)
                continue  # 次のリクエストへ
        
        time.sleep(3)  # レート制限対策
        
        # 進捗表示
        if total_count % 50 == 0:
            print(f"\\n  進捗: {total_count}日処理済み (成功: {success_count}, スキップ: {skip_count})\\n")
    
    print(f"\\nLedger Detail完了: 成功 {success_count}件, スキップ {skip_count}件, 合計 {total_count}件")


if __name__ == "__main__":
    print("Ledger Detail - Historical Data Backfill")
    print("=" * 60)
    
    try:
        backfill()
        
        print("\\n" + "=" * 60)
        print("バックフィル完了")
        
    except KeyboardInterrupt:
        print("\\n\\n中断されました")
    except Exception as e:
        print(f"\\nエラー: {e}")
        import traceback
        traceback.print_exc()
