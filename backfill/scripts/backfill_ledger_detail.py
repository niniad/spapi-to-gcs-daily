"""
Ledger Detail View Data Report - Historical Data Backfill

This script fetches Ledger Detail reports for the past 18 months on a monthly basis.
Data is saved to backfill/data/ledger-detail/.
"""

import time
import gzip
import io
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.http_retry import request_with_retry
from backfill.scripts.auth import get_access_token


# ===================================================================
# Settings
# ===================================================================
MARKETPLACE_ID = "A1VC38T7YXB528"  # Japan
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
DATA_DIR = Path(__file__).parent.parent / "data" / "ledger-detail"

# Backfill period (past 18 months)
BACKFILL_MONTHS = 18


def get_all_month_ranges():
    """
    Generates month ranges for the past 18 months.
    
    Yields:
        tuple: (year, month, start_date_str, end_date_str)
    """
    utc_now = datetime.now(timezone.utc)
    # Start from previous month
    current_date = (utc_now.replace(day=1) - timedelta(days=1)).replace(day=1)
    cutoff_date = utc_now - timedelta(days=30 * BACKFILL_MONTHS)
    
    while current_date >= cutoff_date:
        year = current_date.year
        month = current_date.month
        
        # Calculate end of month
        if month == 12:
            next_month = current_date.replace(year=year + 1, month=1)
        else:
            next_month = current_date.replace(month=month + 1)
        
        # APIリクエスト用の期間（対象月）を計算
        # Ledger Detailはズレがないため、そのまま対象月を指定する
        # 例: 2024-11のデータが欲しい場合、2024-11-01 ~ 2024-11-30 を指定する
        req_start_date = current_date
        req_end_date = next_month - timedelta(days=1)
        
        start_date_str = req_start_date.strftime('%Y-%m-%d')
        end_date_str = req_end_date.strftime('%Y-%m-%d')
        
        yield (year, month, start_date_str, end_date_str)
        
        # Move to previous month
        current_date = (current_date - timedelta(days=1)).replace(day=1)


def fetch_report(year, month, start_date_str, end_date_str, headers):
    """
    Fetches the report for the specified month.
    
    Args:
        year: Year
        month: Month
        start_date_str: Start date string
        end_date_str: End date string
        headers: HTTP headers
        
    Returns:
        tuple: (content, is_rate_limited)
            content (str): Report content (TSV format), or None
            is_rate_limited (bool): Whether rate limited
    """
    # Create report request
    payload = f'''{{
        "reportType": "GET_LEDGER_DETAIL_VIEW_DATA",
        "dataStartTime": "{start_date_str}T00:00:00.00+09:00",
        "dataEndTime": "{end_date_str}T23:59:59.00+09:00",
        "marketplaceIds": ["{MARKETPLACE_ID}"]
    }}'''
    
    try:
        # Create report
        response = request_with_retry(
            'POST',
            f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
            headers=headers,
            data=payload,
            max_retries=5
        )
        report_id = response.json()["reportId"]
        
        # Wait for report completion
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
                print(f"      Report processing failed (Status: {status})")
                return None, False
        
        if not report_document_id:
            print(f"      Timeout")
            return None, False
        
        # Download report
        get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
        response = request_with_retry('GET', get_doc_url, headers=headers)
        download_url = response.json()["url"]
        
        response = request_with_retry('GET', download_url)
        
        # Try gzip decompression
        try:
            with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                return f.read(), False
        except gzip.BadGzipFile:
            # If not gzip, try multiple encodings
            for encoding in ['utf-8', 'shift-jis', 'cp932']:
                try:
                    return response.content.decode(encoding), False
                except UnicodeDecodeError:
                    continue
            # If all fail
            print(f"      Error: Failed to detect encoding")
            return None, False
        except UnicodeDecodeError:
            # gzip but not utf-8
            try:
                with gzip.open(io.BytesIO(response.content), 'rt', encoding='shift-jis') as f:
                    return f.read(), False
            except Exception:
                try:
                    with gzip.open(io.BytesIO(response.content), 'rt', encoding='cp932') as f:
                        return f.read(), False
                except Exception as e:
                    print(f"      Error: gzip decompression failed: {e}")
                    return None, False
    
    except Exception as e:
        # Return flag if 429 error
        if "429" in str(e):
            print(f"      Rate limit (429) detected.")
            return None, True
            
        print(f"      Error: {e}")
        return None, False


def backfill():
    """Executes backfill for Ledger Detail data."""
    print("\\n=== Ledger Detail Backfill Start ===")
    
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
            print(f"  [SKIP] {filename} (Exists)")
            skip_count += 1
            continue
        
        print(f"  [Fetching] {filename}")
        content, is_rate_limited = fetch_report(year, month, start_date_str, end_date_str, headers)
        
        if content and content.strip():
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"    ✓ Saved")
            success_count += 1
            consecutive_errors = 0
        else:
            print(f"    No data")
            skip_count += 1
            consecutive_errors += 1
            
            # Increase wait time on consecutive errors
            if consecutive_errors >= max_consecutive_errors:
                print(f"\\n  {max_consecutive_errors} consecutive errors. Waiting 60s...")
                time.sleep(60)
                consecutive_errors = 0
            elif consecutive_errors >= 3:
                wait_time = min(30, consecutive_errors * 5)
                print(f"  Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
        
        # Wait longer on rate limit
        if is_rate_limited:
            print(f"  Waiting 120s to avoid rate limit...")
            time.sleep(120)
        else:
            time.sleep(90)  # Standard wait time 50s
    
    print(f"\\nLedger Detail Completed: Success {success_count}, Skipped {skip_count}")


if __name__ == "__main__":
    print("Ledger Detail - Historical Data Backfill")
    print("=" * 60)
    
    try:
        backfill()
        
        print("\\n" + "=" * 60)
        print("Backfill Completed")
        
    except KeyboardInterrupt:
        print("\\n\\nInterrupted")
    except Exception as e:
        print(f"\\nError: {e}")
        import traceback
        traceback.print_exc()
