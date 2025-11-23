"""
SP-API Data Acquisition Orchestrator

このファイルは、複数のSP-APIエンドポイントを順次実行するオーケストレーターです。
Cloud Run Functionsのエントリポイントとして機能します。

テスト実行:
- 全エンドポイント実行: https://your-cloud-run-url
- 特定エンドポイントのみ: https://your-cloud-run-url?endpoint=sales_and_traffic
- 特定エンドポイントのみ: https://your-cloud-run-url?endpoint=settlement_report
"""

from endpoints import sales_and_traffic_report, settlement_report, brand_analytics_search_query_performance_report


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
        # クエリパラメータから実行するエンドポイントを取得
        endpoint = request.args.get('endpoint', None)
        
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
            
            elif endpoint == 'brand_analytics_report':
                brand_analytics_search_query_performance_report.run()
                print("\n" + "=" * 60)
                print("処理完了: Brand Analytics Search Query Performance Report")
                print("=" * 60)
                return ("Brand Analytics Report - OK", 200)
            
            else:
                error_msg = f"不明なエンドポイント: {endpoint}"
                print(f"\nError: {error_msg}")
                print("利用可能なエンドポイント: sales_and_traffic, settlement_report, brand_analytics_report")
                return (error_msg, 400)
        
        else:
            # 全エンドポイントを順次実行(本番モード)
            print("\n[本番モード] 全エンドポイントを順次実行")
            
            # 1. Sales and Traffic Report
            sales_and_traffic_report.run()
            
            # 2. Settlement Report
            settlement_report.run()

            # 3. Brand Analytics Search Query Performance Report
            brand_analytics_search_query_performance_report.run()
            
            # 将来追加予定:
            # 4. Orders API
            # orders_api.run()
            # 5. Catalog API
            # catalog_api.run()
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
    main(MockRequest('brand_analytics_report'))