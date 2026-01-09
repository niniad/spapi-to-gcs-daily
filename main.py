"""
SP-API Data Acquisition Orchestrator

このファイルは、複数のSP-APIエンドポイントを順次実行するオーケストレーターです。
Cloud Run Functionsのエントリポイントとして機能します。

テスト実行:
- 全エンドポイント実行: https://your-cloud-run-url
- 特定エンドポイントのみ: https://your-cloud-run-url?endpoint=sales_and_traffic
- 特定エンドポイントのみ: https://your-cloud-run-url?endpoint=settlement_report
"""

import time
from endpoints import sales_and_traffic_report, settlement_report, brand_analytics_search_query_performance_report_weekly, brand_analytics_search_query_performance_report_monthly, brand_analytics_repeat_purchase_report_weekly, brand_analytics_repeat_purchase_report_monthly, ledger_detail_view_data, ledger_summary_view_data, fba_inventory, catalog_items, all_orders_report, orders_api


def main(request):
    """
    Cloud Run Functionsのメインエントリポイント
    
    Args:
        request: Flask request object
        
    Returns:
        tuple: (message, status_code)
    """
    print("=" * 60)
    print("SP-API Data Acquisition - 処理開始")
    print("=" * 60)
    
    try:
        # クエリパラメータまたはJSONボディから実行するエンドポイントを取得
        endpoint = request.args.get('endpoint', None)
        if not endpoint:
            request_json = request.get_json(silent=True)
            if request_json and 'endpoint' in request_json:
                endpoint = request_json['endpoint']
        
        if endpoint:
            # 特定のエンドポイントのみ実行(テストモード)
            print(f"\n[テストモード] エンドポイント指定: {endpoint}")
            
            if endpoint == 'sales_and_traffic':
                sales_and_traffic_report.run()
                print("\n" + "=" * 60)
                print("処理完了: Sales and Traffic Report")
                print("=" * 60)
                return ("Sales and Traffic Report - OK", 200)
            
            elif endpoint == 'settlement_report':
                settlement_report.run()
                print("\n" + "=" * 60)
                print("処理完了: Settlement Report")
                print("=" * 60)
                return ("Settlement Report - OK", 200)
            
            elif endpoint == 'brand_analytics_report_weekly':
                brand_analytics_search_query_performance_report_weekly.run()
                print("\n" + "=" * 60)
                print("処理完了: Brand Analytics Search Query Performance Report (WEEK)")
                print("=" * 60)
                return ("Brand Analytics Report (WEEK) - OK", 200)

            elif endpoint == 'brand_analytics_report_monthly':
                brand_analytics_search_query_performance_report_monthly.run()
                print("\n" + "=" * 60)
                print("処理完了: Brand Analytics Search Query Performance Report (MONTH)")
                print("=" * 60)
                return ("Brand Analytics Report (MONTH) - OK", 200)
            
            elif endpoint == 'ledger_detail':
                ledger_detail_view_data.run()
                print("\n" + "=" * 60)
                print("処理完了: Ledger Detail View Data Report")
                print("=" * 60)
                return ("Ledger Detail Report - OK", 200)

            elif endpoint == 'ledger_summary':
                ledger_summary_view_data.run()
                print("\n" + "=" * 60)
                print("処理完了: Ledger Summary View Data Report")
                print("=" * 60)
                return ("Ledger Summary Report - OK", 200)
            

            
            elif endpoint == 'fba_inventory':
                fba_inventory.run()
                print("\n" + "=" * 60)
                print("処理完了: FBA Inventory")
                print("=" * 60)
                return ("FBA Inventory - OK", 200)
            
            elif endpoint == 'catalog_items':
                catalog_items.run()
                print("\n" + "=" * 60)
                print("処理完了: Catalog Items")
                print("=" * 60)
                return ("Catalog Items - OK", 200)
            
            elif endpoint == 'all_orders_report':
                all_orders_report.run()
                print("\n" + "=" * 60)
                print("処理完了: All Orders Report")
                print("=" * 60)
                return ("All Orders Report - OK", 200)



            elif endpoint == 'brand_analytics_repeat_purchase_report_weekly':
                brand_analytics_repeat_purchase_report_weekly.run()
                print("\n" + "=" * 60)
                print("処理完了: BA Repeat Purchase (Weekly)")
                print("=" * 60)
                return ("BA Repeat Purchase (Weekly) - OK", 200)

            elif endpoint == 'brand_analytics_repeat_purchase_report_monthly':
                brand_analytics_repeat_purchase_report_monthly.run()
                print("\n" + "=" * 60)
                print("処理完了: BA Repeat Purchase (Monthly)")
                print("=" * 60)
                return ("BA Repeat Purchase (Monthly) - OK", 200)
            
            else:
                error_msg = f"不明なエンドポイント: {endpoint}"
                print(f"\nError: {error_msg}")
                print("利用可能なエンドポイント: sales_and_traffic, settlement_report, brand_analytics_report_weekly, brand_analytics_report_monthly, brand_analytics_repeat_purchase_report_weekly, brand_analytics_repeat_purchase_report_monthly, ledger_detail, ledger_summary, transactions, fba_inventory, catalog_items, all_orders_report, orders_api")
                return (error_msg, 400)
        
        else:
            # 全エンドポイントを順次実行(本番モード)
            print("\n[本番モード] 全エンドポイントを順次実行")
            
            # 1. Sales and Traffic Report
            sales_and_traffic_report.run()
            
            # 2. Settlement Report
            settlement_report.run()

            # 3. Brand Analytics Search Query Performance Report
            brand_analytics_search_query_performance_report_weekly.run()
            brand_analytics_search_query_performance_report_monthly.run()

            # 3-2. Brand Analytics Repeat Purchase Report
            brand_analytics_repeat_purchase_report_weekly.run()
            brand_analytics_repeat_purchase_report_monthly.run()

            # 4. Ledger Detail View Data Report
            ledger_detail_view_data.run()

            # 5. Ledger Summary View Data Report
            print("  -> クールダウン: 60秒待機中...")
            time.sleep(60)
            ledger_summary_view_data.run()
            

            
            # 7. FBA Inventory
            fba_inventory.run()
            
            # 8. Catalog Items
            catalog_items.run()
            
            # 9. All Orders Report
            all_orders_report.run()
            
            # 10. Orders API
            orders_api.run()
            # など...
            
            print("\n" + "=" * 60)
            print("全エンドポイント処理完了")
            print("=" * 60)
            return ("All endpoints - OK", 200)
    
    except Exception as e:
        error_msg = f"致命的なエラーが発生しました: {e}"
        print(f"\nError: {error_msg}")
        import traceback
        traceback.print_exc()
        return ("Internal Server Error", 500)


if __name__ == "__main__":
    # ローカルテスト用
    # functions-framework --target=main --signature-type=http --debug
    print("ローカルテスト実行")
    
    class MockRequest:
        def __init__(self, endpoint=None):
            self.args = {'endpoint': endpoint} if endpoint else {}
    
    # テスト: 全エンドポイント実行
    # main(MockRequest())
    
    # テスト: Sales and Traffic Reportのみ
    # main(MockRequest('sales_and_traffic'))
    
    # テスト: Settlement Reportのみ
    # main(MockRequest('settlement_report'))

    # テスト: Brand Analytics Reportのみ
    # main(MockRequest('brand_analytics_report'))

    # テスト: Ledger Detail Reportのみ
    # main(MockRequest('ledger_detail'))

    # テスト: Ledger Summary Reportのみ
    # main(MockRequest('ledger_summary'))

    # テスト: All Orders Reportのみ
    # main(MockRequest('all_orders_report'))