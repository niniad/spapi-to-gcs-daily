"""
Sales and Traffic Report Module

This module retrieves the Sales and Traffic Report from the SP-API and saves it to GCS.
- DAY report: Daily sales and traffic data
- CHILD ASIN report: Sales and traffic data for each child ASIN
"""

import json
import time
import gzip
import io
import logging
from datetime import datetime, timedelta, timezone
from google.cloud import storage
from utils.sp_api_auth import get_access_token
from utils.http_retry import request_with_retry

# ===================================================================
# Configuration
# ===================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MARKETPLACE_ID = "A1VC38T7YXB528"  # Japan
START_DAYS_AGO = 8
END_DAYS_AGO = 1
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"

REPORT_CONFIGS = [
    {
        "type": "DAY",
        "gcs_bucket_name": "sp-api-bucket",
        "gcs_file_prefix": "sales-and-traffic-report/day/",
        "report_options": {}
    },
    {
        "type": "CHILD_ASIN",
        "gcs_bucket_name": "sp-api-bucket",
        "gcs_file_prefix": "sales-and-traffic-report/child-asin/",
        "report_options": {
            "dateGranularity": "DAY",
            "asinGranularity": "CHILD"
        }
    }
]

def _upload_to_gcs(bucket_name, blob_name, content):
    """Uploads a file to GCS."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content, content_type='application/json')
        logging.info(f"Successfully saved to GCS: gs://{bucket_name}/{blob_name}")
    except Exception:
        logging.error(f"Failed to upload to GCS: gs://{bucket_name}/{blob_name}", exc_info=True)

def run():
    """Executes the process of retrieving and saving the Sales and Traffic Report."""
    logging.info("Sales and Traffic Report - Processing started")
    
    try:
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': access_token
        }
        
        utc_now = datetime.now(timezone.utc)
        start_date = utc_now - timedelta(days=START_DAYS_AGO)
        end_date = utc_now - timedelta(days=END_DAYS_AGO)
        logging.info(f"Data acquisition period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        pending_reports = []
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            logging.info(f"Requesting report creation for [{date_str}]...")
            
            for config in REPORT_CONFIGS:
                try:
                    payload_dict = {
                        "marketplaceIds": [MARKETPLACE_ID],
                        "reportType": "GET_SALES_AND_TRAFFIC_REPORT",
                        "dataStartTime": f"{date_str}T00:00:00Z",
                        "dataEndTime": f"{date_str}T23:59:59Z",
                    }
                    if config["report_options"]:
                        payload_dict["reportOptions"] = config["report_options"]
                    
                    payload = json.dumps(payload_dict)
                    response = request_with_retry(
                        'POST',
                        f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
                        headers=headers,
                        data=payload
                    )
                    report_id = response.json()["reportId"]
                    logging.info(f"  -> [{config['type']}] Request OK (Report ID: {report_id})")
                    
                    pending_reports.append({
                        "report_id": report_id,
                        "config": config,
                        "date_str": date_str,
                        "current_date": current_date
                    })
                    
                    time.sleep(2)
                except Exception:
                    logging.error(f"  -> Error: [{config['type']}] Request failed for date {date_str}", exc_info=True)
            
            current_date += timedelta(days=1)

        logging.info(f"--- Waiting for report generation (Target: {len(pending_reports)} reports) ---")
        max_loops = 40
        completed_reports = set()
        
        for i in range(max_loops):
            if len(completed_reports) == len(pending_reports):
                logging.info("All report processing is complete.")
                break
                
            logging.info(f"Checking status (Attempt {i+1}/{max_loops})...")
            
            for item in pending_reports:
                report_id = item['report_id']
                if report_id in completed_reports:
                    continue
                
                try:
                    get_report_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/reports/{report_id}"
                    response = request_with_retry('GET', get_report_url, headers=headers)
                    status = response.json().get("processingStatus")
                    
                    if status == "DONE":
                        logging.info(f"  -> Report {report_id} ({item['date_str']} {item['config']['type']}): DONE")
                        
                        report_document_id = response.json()["reportDocumentId"]
                        get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
                        doc_response = request_with_retry('GET', get_doc_url, headers=headers)
                        download_url = doc_response.json()["url"]
                        
                        dl_response = request_with_retry('GET', download_url)
                        with gzip.open(io.BytesIO(dl_response.content), 'rt', encoding='utf-8') as f:
                            report_content = f.read()
                        
                        if report_content.strip():
                            blob_name = f"{item['config']['gcs_file_prefix']}{item['current_date'].strftime('%Y%m%d')}.json"
                            _upload_to_gcs(item['config']['gcs_bucket_name'], blob_name, report_content)
                        else:
                            logging.warning(f"    -> Report content for {report_id} is empty. Skipping save.")
                        
                        completed_reports.add(report_id)
                        
                    elif status in ["FATAL", "CANCELLED"]:
                        logging.error(f"  -> Report {report_id} ({item['date_str']}): Failed with status ({status})")
                        completed_reports.add(report_id)
                
                except Exception:
                    logging.error(f"  -> Error processing report {report_id}.", exc_info=True)
            
            if len(completed_reports) == len(pending_reports):
                break
                
            time.sleep(30)

        logging.info("Sales and Traffic Report - Processing finished")
        
    except Exception:
        logging.critical("A fatal error occurred during Sales and Traffic Report processing.", exc_info=True)
        raise
