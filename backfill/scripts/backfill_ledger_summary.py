"""
Ledger Summary View Data Report - Historical Data Backfill

このスクリプトは、過去18ヶ月分のLedger Summaryレポートを月次で取得します。
取得したデータは backfill/data/ledger-summary/ に保存されます。
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
DATA_DIR = Path(__file__).parent.parent / "data" / "ledger-summary"

# バックフィル期間（過去18ヶ月）
BACKFILL_MONTHS = 18


def get_all_month_ranges():
    """
    現在月から過去18ヶ月分の月範囲を生成します。
    
    Yields:
        tuple: (year, month, start_date_str, end_date_str)
    """
    utc_now = datetime.now(timezone.utc)
    # 先月から開始
    current_date = (utc_now.replace(day=1) - timedelta(days=1)).replace(day=1)
    cutoff_date = utc_now - timedelta(days=30 * BACKFILL_MONTHS)
    
    while current_date >= cutoff_date:
        year = current_date.year
        month = current_date.month
        
        # 月の最終日を計算
        if month == 12:
            next_month = current_date.replace(year=year + 1, month=1)
        else:
            next_month = current_date.replace(month=month + 1)
        
        # end_dateは月の最終日（翌月の初日 - 1日）
        last_day_of_month = next_month - timedelta(days=1)
        
        start_date_str = current_date.strftime('%Y-%m-%d')
        end_date_str = last_day_of_month.strftime('%Y-%m-%d')
        
        yield (year, month, start_date_str, end_date_str)
        
        # 前月へ
        current_date = (current_date - timedelta(days=1)).replace(day=1)


def fetch_report(year, month, start_date_str, end_date_str, headers):
    """
    指定月のレポートを取得します。
    
    Args:
        year: 年
        month: 月
        start_date_str: 開始日文字列
        end_date_str: 終了日文字列
        headers: HTTPヘッダー
        
    Returns:
        tuple: (content, is_rate_limited)
            content (str): レポート内容（TSV形式）、またはNone
            is_rate_limited (bool): レート制限にかかったかどうか
    """
    # レポート作成リクエスト
    payload = f'''{{
        "reportType": "GET_LEDGER_SUMMARY_VIEW_DATA",
        "dataStartTime": "{start_date_str}T00:00:00.00+09:00",
        "dataEndTime": "{end_date_str}T23:59:59.00+09:00",
        "marketplaceIds": ["{MARKETPLACE_ID}"],
        "reportOptions": {{
            "aggregatedByTimePeriod": "MONTHLY"
        }}
    }}'''
    
    try:
        # レポート作成
        response = request_with_retry(
            'POST',
            f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
            headers=headers,
            data=payload,
            max_retries=10
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
                max_retries=10
            )
            status = response.json().get("processingStatus")
            
            if status == "DONE":
                report_document_id = response.json()["reportDocumentId"]
                break
            elif status in ["FATAL", "CANCELLED"]:
                print(f"      レポート処理失敗 (Status: {status})")
                return None, False
        
        if not report_document_id:
            print(f"      タイムアウト")
            return None, False
        
        # レポートダウンロード
        get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
        response = request_with_retry('GET', get_doc_url, headers=headers)
        download_url = response.json()["url"]
        
        response = request_with_retry('GET', download_url)
        
        # gzip解凍を試行
        try:
            with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                return f.read(), False
        except gzip.BadGzipFile:
            # gzipでない場合、複数のエンコーディングを試行
            for encoding in ['utf-8', 'shift-jis', 'cp932']:
                try:
                    return response.content.decode(encoding), False
                except UnicodeDecodeError:
                    continue
            # すべて失敗した場合
            print(f"      エラー: エンコーディングの検出に失敗しました")
            return None, False
        except UnicodeDecodeError:
            # gzipだがutf-8でない場合
            try:
                with gzip.open(io.BytesIO(response.content), 'rt', encoding='shift-jis') as f:
                    return f.read(), False
            except Exception:
                try:
                    with gzip.open(io.BytesIO(response.content), 'rt', encoding='cp932') as f:
                        return f.read(), False
                except Exception as e:
                    print(f"      エラー: gzip解凍失敗: {e}")
                    return None, False
    
    except Exception as e:
        # 429エラーの場合はフラグを立てて返す
        if "429" in str(e):
            print(f"      レート制限(429)を検知しました。")
            return None, True
            
        print(f"      エラー: {e}")
        return None, False


def backfill():
    """Ledger Summaryデータのバックフィルを実行します。"""
    print("\\n=== Ledger Summary データのバックフィル開始 ===")
    
    access_token = get_access_token()
    headers = {
        'Content-Type': 'application/json',
        'x-amz-access-token': access_token
    }
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    success_count = 0
    skip_count = 0
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    for year, month, start_date_str, end_date_str in get_all_month_ranges():
        filename = f"{year:04d}{month:02d}.tsv"
        filepath = DATA_DIR / filename
        
        if filepath.exists():
            print(f"  [SKIP] {filename} (既存)")
            skip_count += 1
            continue
        
        print(f"  [取得中] {filename}")
        content, is_rate_limited = fetch_report(year, month, start_date_str, end_date_str, headers)
        
        if content and content.strip():
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"    ✓ 保存完了")
            success_count += 1
            consecutive_errors = 0
        else:
            print(f"    データなし")
            skip_count += 1
            consecutive_errors += 1
            
            # 連続エラー時は待機時間を増やす
            if consecutive_errors >= max_consecutive_errors:
                print(f"\\n  連続エラーが{max_consecutive_errors}回発生しました。60秒待機します...")
                time.sleep(60)
                consecutive_errors = 0
            elif consecutive_errors >= 3:
                wait_time = min(30, consecutive_errors * 5)
                print(f"  {wait_time}秒待機します...")
                time.sleep(wait_time)
                time.sleep(wait_time)
                continue
        
        # レート制限時は長めに待機
        if is_rate_limited:
            print(f"  レート制限回避のため120秒待機します...")
            time.sleep(120)
        else:
            time.sleep(90)  # 通常時の待機時間を50秒に設定（理論値45秒+バッファ）
    
    print(f"\\nLedger Summary完了: 成功 {success_count}件, スキップ {skip_count}件")


if __name__ == "__main__":
    print("Ledger Summary - Historical Data Backfill")
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
