"""
FBA Inventory Module

This module uses the SP-API's FBA Inventory API to retrieve inventory information and saves it to GCS.
It also extracts a list of ASINs from the inventory information for use by other endpoints.
"""

import json
import logging
from datetime import datetime
from google.cloud import storage
from utils.sp_api_auth import get_access_token
from utils.http_retry import request_with_retry

# ===================================================================
# Configuration
# ===================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MARKETPLACE_ID = "A1VC38T7YXB528"  # Japan
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
GCS_BUCKET_NAME = "sp-api-bucket"
GCS_FILE_PREFIX = "fba-inventory/"


def _upload_to_gcs(bucket_name, blob_name, content):
    """
    Uploads a file to GCS.
    
    Args:
        bucket_name: GCS bucket name.
        blob_name: The name of the file to save.
        content: The content of the file (JSON string).
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content, content_type='application/jsonl')
        logging.info(f"Successfully saved to GCS: gs://{bucket_name}/{blob_name}")
    except Exception:
        logging.error(f"Failed to upload to GCS: gs://{bucket_name}/{blob_name}", exc_info=True)

def _fetch_inventory_summaries(access_token, next_token=None):
    """
    Fetches inventory summaries from the FBA Inventory API.
    
    Args:
        access_token: SP-API access token.
        next_token: Token for pagination (optional).
        
    Returns:
        dict: API response.
    """
    url = f"{SP_API_ENDPOINT}/fba/inventory/v1/summaries"
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    params = {
        "marketplaceIds": MARKETPLACE_ID,
        "granularityType": "Marketplace",
        "granularityId": MARKETPLACE_ID,
        "details": "true" # Include details
    }
    if next_token:
        params["nextToken"] = next_token
    
    response = request_with_retry("GET", url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        error_msg = f"FBA Inventory API Error: {response.status_code} - {response.text}"
        logging.error(error_msg)
        raise Exception(error_msg)

def _get_all_inventory_summaries(access_token):
    """
    Retrieves all inventory summaries (handles pagination).
    
    Args:
        access_token: SP-API access token.
        
    Returns:
        list: A list of all inventory summaries.
    """
    all_summaries = []
    next_token = None
    page = 1
    
    logging.info("Fetching FBA inventory information...")
    
    while True:
        logging.info(f"Fetching page {page}...")
        response_data = _fetch_inventory_summaries(access_token, next_token)
        
        # Handle payload wrapper if present
        payload = response_data.get("payload", response_data)
        summaries = payload.get("inventorySummaries", [])
            
        all_summaries.extend(summaries)
        logging.info(f"Fetched {len(summaries)} inventory items.")
        
        pagination = response_data.get("pagination", {})
        next_token = pagination.get("nextToken")
        
        if not next_token:
            break
        page += 1
    
    logging.info(f"Finished fetching all inventory. Total items: {len(all_summaries)}")
    return all_summaries

def get_asin_list():
    """
    Retrieves a list of ASINs from FBA inventory.
    
    Returns:
        list: A list of ASINs.
    """
    try:
        access_token = get_access_token()
        summaries = _get_all_inventory_summaries(access_token)
        
        asin_set = {summary['asin'] for summary in summaries if 'asin' in summary}
        asin_list = sorted(list(asin_set))
        
        logging.info(f"Extracted {len(asin_list)} unique ASINs.")
        return asin_list
        
    except Exception:
        logging.error("Failed to get ASIN list.", exc_info=True)
        raise

def run():
    """
    Fetches FBA inventory information and saves it to GCS.
    """
    logging.info("FBA Inventory - Processing started")
    
    try:
        access_token = get_access_token()
        summaries = _get_all_inventory_summaries(access_token)
        
        if not summaries:
            logging.warning("No inventory information found.")
            return
        
        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"{GCS_FILE_PREFIX}{current_date}.jsonl"
        
        ndjson_lines = []
        for summary in summaries:
            item_data = {
                "fetchedAt": datetime.now().isoformat(),
                "marketplaceId": MARKETPLACE_ID,
                "inventorySummary": summary
            }
            ndjson_lines.append(json.dumps(item_data, ensure_ascii=False))
            
        ndjson_content = "\n".join(ndjson_lines)
        
        _upload_to_gcs(GCS_BUCKET_NAME, filename, ndjson_content)
        
        unique_asins = sorted({s.get("asin") for s in summaries if s.get("asin")})
        logging.info(f"Unique ASINs count: {len(unique_asins)}")
        logging.info(f"ASIN list: {', '.join(unique_asins[:10])}{'...' if len(unique_asins) > 10 else ''}")
        
        logging.info("FBA Inventory - Processing finished")
        
    except Exception:
        logging.error("An error occurred during FBA Inventory processing.", exc_info=True)
        raise

if __name__ == "__main__":
    run()
