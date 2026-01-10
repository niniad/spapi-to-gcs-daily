"""
SP-API Data Acquisition Orchestrator

This file acts as an orchestrator to run multiple SP-API endpoint tasks.
It can run all tasks in parallel (production mode) or a single task (test mode).

Execution (Cloud Run):
- All endpoints: https://your-cloud-run-url
- Specific endpoint: https://your-cloud-run-url?endpoint=sales_and_traffic
"""

import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from endpoints import (
    sales_and_traffic_report, 
    settlement_report, 
    brand_analytics_search_query_performance_report_weekly, 
    brand_analytics_search_query_performance_report_monthly, 
    brand_analytics_repeat_purchase_report_weekly, 
    brand_analytics_repeat_purchase_report_monthly, 
    ledger_detail_view_data, 
    ledger_summary_view_data, 
    fba_inventory, 
    catalog_items, 
    all_orders_report, 
    orders_api
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Endpoint Mapping ---
# Maps a string name to the corresponding module and run function
ENDPOINT_MAP = {
    "fba_inventory": fba_inventory,
    "catalog_items": catalog_items,
    "sales_and_traffic": sales_and_traffic_report,
    "settlement_report": settlement_report,
    "brand_analytics_search_query_weekly": brand_analytics_search_query_performance_report_weekly,
    "brand_analytics_search_query_monthly": brand_analytics_search_query_performance_report_monthly,
    "brand_analytics_repeat_purchase_weekly": brand_analytics_repeat_purchase_report_weekly,
    "brand_analytics_repeat_purchase_monthly": brand_analytics_repeat_purchase_report_monthly,
    "ledger_detail": ledger_detail_view_data,
    "ledger_summary": ledger_summary_view_data,
    "all_orders_report": all_orders_report,
    "orders_api": orders_api,
}

def run_task(endpoint_name, module):
    """Wrapper to run a task and handle its outcome."""
    try:
        logging.info(f"Starting task: {endpoint_name}")
        module.run()
        logging.info(f"Finished task: {endpoint_name}")
        return endpoint_name, "SUCCESS"
    except Exception as e:
        logging.error(f"Task failed: {endpoint_name}", exc_info=True)
        return endpoint_name, f"FAILED: {e}"

def main(request):
    """
    Main entry point for the Cloud Function.
    Args:
        request: Flask request object
    Returns:
        tuple: (message, status_code)
    """
    logging.info("SP-API Data Acquisition - Processing started")
    
    try:
        endpoint_param = request.args.get('endpoint')
        request_json = request.get_json(silent=True)
        if not endpoint_param and request_json and 'endpoint' in request_json:
            endpoint_param = request_json['endpoint']

        # --- Test Mode: Run a single endpoint ---
        if endpoint_param:
            if endpoint_param in ENDPOINT_MAP:
                logging.info(f"[Test Mode] Running single endpoint: {endpoint_param}")
                name, result = run_task(endpoint_param, ENDPOINT_MAP[endpoint_param])
                if "SUCCESS" in result:
                    return f"{name} - OK", 200
                else:
                    return f"{name} - {result}", 500
            else:
                logging.error(f"Unknown endpoint: {endpoint_param}")
                return f"Unknown endpoint: {endpoint_param}", 400

        # --- Production Mode: Run all endpoints ---
        else:
            logging.info("[Production Mode] Running all endpoints.")
            
            # Step 1: Run fba_inventory first due to dependencies.
            logging.info("Executing prerequisite task: fba_inventory")
            name, result = run_task("fba_inventory", fba_inventory)
            if "FAILED" in result:
                # If the prerequisite fails, we might not want to continue.
                logging.critical("Prerequisite task fba_inventory failed. Aborting parallel execution.")
                return "Prerequisite fba_inventory failed", 500

            # Step 2: Run all other tasks in parallel.
            tasks_to_run_in_parallel = {k: v for k, v in ENDPOINT_MAP.items() if k != "fba_inventory"}
            results = {}

            with ThreadPoolExecutor(max_workers=len(tasks_to_run_in_parallel)) as executor:
                logging.info(f"Submitting {len(tasks_to_run_in_parallel)} tasks to ThreadPoolExecutor.")
                future_to_endpoint = {executor.submit(run_task, name, module): name for name, module in tasks_to_run_in_parallel.items()}
                
                for future in as_completed(future_to_endpoint):
                    endpoint_name = future_to_endpoint[future]
                    try:
                        name, res = future.result()
                        results[name] = res
                        logging.info(f"Result for {name}: {res}")
                    except Exception as e:
                        logging.error(f"Future for {endpoint_name} generated an exception: {e}", exc_info=True)
                        results[endpoint_name] = f"FAILED in future: {e}"

            logging.info("--- All tasks completed ---")
            failed_tasks = {k: v for k, v in results.items() if "FAILED" in v}

            if failed_tasks:
                logging.error(f"Some tasks failed: {failed_tasks}")
                return f"Run finished with failures: {failed_tasks}", 500
            else:
                logging.info("All endpoints processed successfully.")
                return "All endpoints - OK", 200

    except Exception:
        logging.critical("A fatal error occurred in the main orchestrator.", exc_info=True)
        return "Internal Server Error", 500
