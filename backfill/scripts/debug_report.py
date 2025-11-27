
import json
import time
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.http_retry import request_with_retry
from backfill.scripts.auth import get_access_token

MARKETPLACE_ID = "A1VC38T7YXB528"
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"

ASIN_LIST = [
    "B0D894LS44", "B0D89H2L67", "B0D89DTD29", "B0D88XNCHG", "B0DBSM5ZDZ",
    "B0DBSF1CZ6", "B0DBS2WWJN", "B0DBS1ZQ7K", "B0DBS2CK1T", "B0DBSB6XY9",
    "B0DT5P24N2", "B0DT51B33M", "B0FRZ3Z755", "B0FRZ2D3G2"
]

def test_report(start_date, end_date, asins=None):
    print(f"Testing report for {start_date} to {end_date}")
    if asins:
        print(f"With ASINs: {asins[:20]}...")
    else:
        print("With NO ASIN filter (All Brand ASINs)")

    access_token = get_access_token()
    headers = {
        'Content-Type': 'application/json',
        'x-amz-access-token': access_token
    }

    payload_dict = {
        "marketplaceIds": [MARKETPLACE_ID],
        "reportType": "GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT",
        "dataStartTime": f"{start_date}T00:00:00Z",
        "dataEndTime": f"{end_date}T00:00:00Z",
        "reportOptions": {
            "reportPeriod": "WEEK"
        }
    }
    
    if asins:
        payload_dict["reportOptions"]["asin"] = asins

    print(f"Payload: {json.dumps(payload_dict, indent=2)}")

    try:
        response = request_with_retry(
            'POST',
            f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
            headers=headers,
            data=json.dumps(payload_dict),
            max_retries=1
        )
        if response.status_code != 202:
            print(f"Error creating report: {response.status_code} {response.text}")
            return

        report_id = response.json()["reportId"]
        print(f"Report ID: {report_id}")

        # Poll for status
        for i in range(10):
            time.sleep(5)
            resp = request_with_retry(
                'GET',
                f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}",
                headers=headers
            )
            status = resp.json().get("processingStatus")
            print(f"Status: {status}")
            if status in ["DONE", "FATAL", "CANCELLED"]:
                break
        
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    # Test case 1: Monthly report for August 2024 (Boundary month)
    print("\n--- TEST CASE 1: Monthly Report (2024-08) ---")
    
    # Manually construct payload for MONTH test since test_report defaults to WEEK
    start_date = "2024-08-01"
    end_date = "2024-08-31"
    
    print(f"Testing MONTH report for {start_date} to {end_date}")
    access_token = get_access_token()
    headers = {
        'Content-Type': 'application/json',
        'x-amz-access-token': access_token
    }

    payload_dict = {
        "marketplaceIds": [MARKETPLACE_ID],
        "reportType": "GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT",
        "dataStartTime": f"{start_date}T00:00:00Z",
        "dataEndTime": f"{end_date}T00:00:00Z",
        "reportOptions": {
            "reportPeriod": "MONTH"
        }
    }
    
    print(f"Payload: {json.dumps(payload_dict, indent=2)}")

    try:
        response = request_with_retry(
            'POST',
            f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
            headers=headers,
            data=json.dumps(payload_dict),
            max_retries=1
        )
        if response.status_code != 202:
            print(f"Error creating report: {response.status_code} {response.text}")
        else:
            report_id = response.json()["reportId"]
            print(f"Report ID: {report_id}")

            # Poll for status
            for i in range(30):
                time.sleep(5)
                resp = request_with_retry(
                    'GET',
                    f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}",
                    headers=headers
                )
                status = resp.json().get("processingStatus")
                print(f"Status: {status}")
                if status in ["DONE", "FATAL", "CANCELLED"]:
                    break
        
    except Exception as e:
        print(f"Exception: {e}")
