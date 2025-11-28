"""
Brand Analytics Search Query Performance Report - Historical Data Backfill

縺薙・繧ｹ繧ｯ繝ｪ繝励ヨ縺ｯ縲・℃蜴ｻ2蟷ｴ蛻・・Brand Analytics繝ｬ繝昴・繝医ｒ蜿門ｾ励＠縺ｾ縺吶・- WEEK: 騾ｱ谺｡繝・・繧ｿ・域律譖懈律縲懷悄譖懈律・・- MONTH: 譛域ｬ｡繝・・繧ｿ

蜿門ｾ励＠縺溘ョ繝ｼ繧ｿ縺ｯ backfill/data/brand-analytics/ 縺ｫ菫晏ｭ倥＆繧後∪縺吶・"""

import json
import time
import gzip
import io
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# 繝励Ο繧ｸ繧ｧ繧ｯ繝医Ν繝ｼ繝医ｒ繝代せ縺ｫ霑ｽ蜉
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.http_retry import request_with_retry
from backfill.scripts.auth import get_access_token


# ===================================================================
# 險ｭ螳・# ===================================================================
MARKETPLACE_ID = "A1VC38T7YXB528"  # 譌･譛ｬ
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
DATA_DIR = Path(__file__).parent.parent / "data" / "brand-analytics"

# 蟇ｾ雎｡ASIN繝ｪ繧ｹ繝・ASIN_LIST = [
    "B0D894LS44", "B0D89H2L67", "B0D89DTD29", "B0D88XNCHG", "B0DBSM5ZDZ",
    "B0DBSF1CZ6", "B0DBS2WWJN", "B0DBS1ZQ7K", "B0DBS2CK1T", "B0DBSB6XY9",
    "B0DT5P24N2", "B0DT51B33M", "B0FRZ3Z755", "B0FRZ2D3G2"
]

# 繝舌ャ繧ｯ繝輔ぅ繝ｫ譛滄俣・磯℃蜴ｻ2蟷ｴ・・BACKFILL_YEARS = 2


def get_week_range(end_date):
    """
    謖・ｮ壹＆繧後◆邨ゆｺ・律縺九ｉ1騾ｱ髢薙・遽・峇繧定ｨ育ｮ励＠縺ｾ縺呻ｼ域律譖懈律縲懷悄譖懈律・峨・    
    Args:
        end_date: 邨ゆｺ・律・亥悄譖懈律・・        
    Returns:
        tuple: (start_date, end_date)
    """
    start_date = end_date - timedelta(days=6)
    return start_date, end_date


def get_all_week_ranges(start_from_date):
    """
    謖・ｮ壹＆繧後◆譌･莉倥°繧蛾℃蜴ｻ縺ｫ驕｡縺｣縺ｦ縲√☆縺ｹ縺ｦ縺ｮ騾ｱ遽・峇繧堤函謌舌＠縺ｾ縺吶・    
    Args:
        start_from_date: 髢句ｧ区律・域怙譁ｰ縺ｮ蝨滓屆譌･・・        
    Yields:
        tuple: (start_date, end_date) 縺ｮ騾ｱ遽・峇
    """
    current_end = start_from_date
    cutoff_date = start_from_date - timedelta(days=365 * BACKFILL_YEARS)
    
    while current_end >= cutoff_date:
        start, end = get_week_range(current_end)
        if start >= cutoff_date:
            yield (start, end)
        current_end -= timedelta(days=7)


def get_all_month_ranges(start_from_date):
    """
    謖・ｮ壹＆繧後◆譌･莉倥°繧蛾℃蜴ｻ縺ｫ驕｡縺｣縺ｦ縲√☆縺ｹ縺ｦ縺ｮ譛育ｯ・峇繧堤函謌舌＠縺ｾ縺吶・    
    Args:
        start_from_date: 髢句ｧ区律
        
    Yields:
        tuple: (start_date, end_date) 縺ｮ譛育ｯ・峇
    """
    current_date = start_from_date.replace(day=1)
    cutoff_date = start_from_date - timedelta(days=365 * BACKFILL_YEARS)
    
    while current_date >= cutoff_date:
        # 譛医・譛邨よ律繧定ｨ育ｮ・        if current_date.month == 12:
            next_month = current_date.replace(year=current_date.year + 1, month=1)
        else:
            next_month = current_date.replace(month=current_date.month + 1)
        
        month_end = next_month - timedelta(days=1)
        
        yield (current_date, month_end)
        
        # 蜑肴怦縺ｸ
        current_date = (current_date - timedelta(days=1)).replace(day=1)


def fetch_report(period, start_date, end_date, headers):
    """
    繝ｬ繝昴・繝医ｒ蜿門ｾ励＠縺ｾ縺吶・    
    Args:
        period: "WEEK" 縺ｾ縺溘・ "MONTH"
        start_date: 髢句ｧ区律
        end_date: 邨ゆｺ・律
        headers: HTTP繝倥ャ繝繝ｼ
        
    Returns:
        str: 繝ｬ繝昴・繝亥・螳ｹ・・DJSON蠖｢蠑擾ｼ峨√∪縺溘・None
    """
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # 繝ｬ繝昴・繝井ｽ懈・繝ｪ繧ｯ繧ｨ繧ｹ繝・    payload_dict = {
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
        # 繝ｬ繝昴・繝井ｽ懈・
        response = request_with_retry(
            'POST',
            f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
            headers=headers,
            data=payload,
            max_retries=5,
            retry_delay=60
        )
        report_id = response.json()["reportId"]
        
        # 繝ｬ繝昴・繝亥ｮ御ｺ・ｒ蠕・ｩ・        get_report_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}"
        report_document_id = None
        
        for attempt in range(20):  # 譛螟ｧ20蝗櫁ｩｦ陦・            time.sleep(15)
            response = request_with_retry(
                'GET',
                get_report_url,
                headers=headers,
                max_retries=3
            )
            status = response.json().get("processingStatus")
            
            if status == "DONE":
                report_document_id = response.json()["reportDocumentId"]
                break
            elif status in ["FATAL", "CANCELLED"]:
                print(f"      繝ｬ繝昴・繝亥・逅・､ｱ謨・(Status: {status})")
                return None
        
        if not report_document_id:
            print(f"      繧ｿ繧､繝繧｢繧ｦ繝・)
            return None
        
        # 繝ｬ繝昴・繝医ム繧ｦ繝ｳ繝ｭ繝ｼ繝・        get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
        response = request_with_retry('GET', get_doc_url, headers=headers)
        download_url = response.json()["url"]
        
        response = request_with_retry('GET', download_url)
        with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
            report_content = f.read()
        
        # NDJSON蠖｢蠑上↓螟画鋤
        json_data = json.loads(report_content)
        items = json_data.get("dataByAsin", [])
        
        if items:
            ndjson_lines = [json.dumps(item, ensure_ascii=False) for item in items]
            return "\\n".join(ndjson_lines)
        else:
            print(f"      繝・・繧ｿ縺ｪ縺・)
            return None
    
    except Exception as e:
        print(f"      繧ｨ繝ｩ繝ｼ: {e}")
        return None


def backfill_weekly():
    """騾ｱ谺｡繝・・繧ｿ縺ｮ繝舌ャ繧ｯ繝輔ぅ繝ｫ繧貞ｮ溯｡後＠縺ｾ縺吶・""
    print("\\n=== 騾ｱ谺｡繝・・繧ｿ縺ｮ繝舌ャ繧ｯ繝輔ぅ繝ｫ髢句ｧ・===")
    
    # 譛譁ｰ縺ｮ蝨滓屆譌･繧定ｨ育ｮ・    utc_now = datetime.now(timezone.utc)
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
    max_consecutive_errors = 5
    
    for start_date, end_date in get_all_week_ranges(latest_saturday):
        filename = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}.json"
        filepath = week_dir / filename
        
        if filepath.exists():
            print(f"  [SKIP] {filename} (譌｢蟄・")
            skip_count += 1
            continue
        
        print(f"  [蜿門ｾ嶺ｸｭ] {filename}")
        content = fetch_report("WEEK", start_date, end_date, headers)
        
        if content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"    笨・菫晏ｭ伜ｮ御ｺ・)
            success_count += 1
            consecutive_errors = 0
        else:
            skip_count += 1
            consecutive_errors += 1
            
            # 騾｣邯壹お繝ｩ繝ｼ譎ゅ・蠕・ｩ滓凾髢薙ｒ蠅励ｄ縺・            if consecutive_errors >= max_consecutive_errors:
                print(f"\\n  騾｣邯壹お繝ｩ繝ｼ縺鶏max_consecutive_errors}蝗樒匱逕溘＠縺ｾ縺励◆縲・0遘貞ｾ・ｩ溘＠縺ｾ縺・..")
                time.sleep(60)
                consecutive_errors = 0
            elif consecutive_errors >= 3:
                wait_time = min(30, consecutive_errors * 5)
                print(f"  {wait_time}遘貞ｾ・ｩ溘＠縺ｾ縺・..")
                time.sleep(wait_time)
                continue
        
        time.sleep(3)  # 繝ｬ繝ｼ繝亥宛髯仙ｯｾ遲・    
    print(f"\\n騾ｱ谺｡繝・・繧ｿ螳御ｺ・ 謌仙粥 {success_count}莉ｶ, 繧ｹ繧ｭ繝・・ {skip_count}莉ｶ")


def backfill_monthly():
    """譛域ｬ｡繝・・繧ｿ縺ｮ繝舌ャ繧ｯ繝輔ぅ繝ｫ繧貞ｮ溯｡後＠縺ｾ縺吶・""
    print("\\n=== 譛域ｬ｡繝・・繧ｿ縺ｮ繝舌ャ繧ｯ繝輔ぅ繝ｫ髢句ｧ・===")
    
    utc_now = datetime.now(timezone.utc)
    # 蜈域怦縺ｮ譛ｫ譌･
    this_month_first = utc_now.replace(day=1)
    last_month_end = this_month_first - timedelta(days=1)
    
    access_token = get_access_token()
    headers = {
        'Content-Type': 'application/json',
        'x-amz-access-token': access_token
    }
    
    month_dir = DATA_DIR / "MONTH"
    month_dir.mkdir(parents=True, exist_ok=True)
    
    success_count = 0
    skip_count = 0
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    for start_date, end_date in get_all_month_ranges(last_month_end):
        filename = f"{start_date.strftime('%Y%m')}.json"
        filepath = month_dir / filename
        
        if filepath.exists():
            print(f"  [SKIP] {filename} (譌｢蟄・")
            skip_count += 1
            continue
        
        print(f"  [蜿門ｾ嶺ｸｭ] {filename}")
        content = fetch_report("MONTH", start_date, end_date, headers)
        
        if content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"    笨・菫晏ｭ伜ｮ御ｺ・)
            success_count += 1
            consecutive_errors = 0
        else:
            skip_count += 1
            consecutive_errors += 1
            
            # 騾｣邯壹お繝ｩ繝ｼ譎ゅ・蠕・ｩ滓凾髢薙ｒ蠅励ｄ縺・            if consecutive_errors >= max_consecutive_errors:
                print(f"\\n  騾｣邯壹お繝ｩ繝ｼ縺鶏max_consecutive_errors}蝗樒匱逕溘＠縺ｾ縺励◆縲・0遘貞ｾ・ｩ溘＠縺ｾ縺・..")
                time.sleep(60)
                consecutive_errors = 0
            elif consecutive_errors >= 3:
                wait_time = min(30, consecutive_errors * 5)
                print(f"  {wait_time}遘貞ｾ・ｩ溘＠縺ｾ縺・..")
                time.sleep(wait_time)
                continue
        
        time.sleep(3)  # 繝ｬ繝ｼ繝亥宛髯仙ｯｾ遲・    
    print(f"\\n譛域ｬ｡繝・・繧ｿ螳御ｺ・ 謌仙粥 {success_count}莉ｶ, 繧ｹ繧ｭ繝・・ {skip_count}莉ｶ")


if __name__ == "__main__":
    print("Brand Analytics - Historical Data Backfill")
    print("=" * 60)
    
    try:
        backfill_weekly()
        backfill_monthly()
        
        print("\\n" + "=" * 60)
        print("縺吶∋縺ｦ縺ｮ繝舌ャ繧ｯ繝輔ぅ繝ｫ螳御ｺ・)
        
    except KeyboardInterrupt:
        print("\\n\\n荳ｭ譁ｭ縺輔ｌ縺ｾ縺励◆")
    except Exception as e:
        print(f"\\n繧ｨ繝ｩ繝ｼ: {e}")
        import traceback
        traceback.print_exc()
