
import os
import sys

# utilsモジュールをインポートできるようにパスを追加
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from utils.sp_api_auth import get_access_token
except ImportError:
    print("Error: utils/sp_api_auth.py が見つかりません。")
    sys.exit(1)

def main():
    print("SP-API 認証テストツール")
    print("--------------------------------")
    print("Cloud Runで発生している401エラーの原因を特定するため、")
    print("手元の認証情報（Refresh Tokenなど）が有効か確認します。")
    print("--------------------------------")

    # 現在の環境変数を表示（値は隠す）
    print("現在の環境変数設定状況:")
    env_vars = ["SP_API_CLIENT_ID", "SP_API_CLIENT_SECRET", "SP_API_REFRESH_TOKEN"]
    for var in env_vars:
        val = os.environ.get(var)
        status = "設定済み" if val else "未設定"
        print(f"  - {var}: {status}")
    print("--------------------------------")

    # 未設定の場合は入力を促す
    if not os.environ.get("SP_API_CLIENT_ID"):
        os.environ["SP_API_CLIENT_ID"] = input("SP_API_CLIENT_ID を入力してください: ").strip()
    
    if not os.environ.get("SP_API_CLIENT_SECRET"):
        os.environ["SP_API_CLIENT_SECRET"] = input("SP_API_CLIENT_SECRET を入力してください: ").strip()
        
    if not os.environ.get("SP_API_REFRESH_TOKEN"):
        os.environ["SP_API_REFRESH_TOKEN"] = input("SP_API_REFRESH_TOKEN を入力してください: ").strip()

    print("\n接続テストを実行中...")
    try:
        token = get_access_token()
        print("\n✅ 成功: アクセストークンが正常に取得できました。")
        print("この認証情報は有効です。Cloud Runの環境変数がこれと一致しているか確認してください。")
    except Exception as e:
        print(f"\n❌ 失敗: アクセストークンの取得に失敗しました。")
        print(f"エラー詳細: {e}")
        print("\nヒント:")
        print("- Refresh Tokenの有効期限が切れている可能性があります（通常1年）。")
        print("- Client ID / Secret が正しいか確認してください。")
        print("- Refresh Tokenを再生成する必要があるかもしれません。")

if __name__ == "__main__":
    main()
