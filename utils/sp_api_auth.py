"""
SP-API Authentication Utility

このモジュールは、SP-APIのアクセストークンを取得する共通機能を提供します。
全てのSP-APIエンドポイントで使用されます。
"""

import os
import requests


def get_access_token():
    """
    リフレッシュトークンからSP-APIアクセストークンを取得します。
    
    環境変数から以下の値を取得します:
    - SP_API_CLIENT_ID: クライアントID
    - SP_API_CLIENT_SECRET: クライアントシークレット
    - SP_API_REFRESH_TOKEN: リフレッシュトークン
    
    Returns:
        str: アクセストークン
        
    Raises:
        ValueError: 環境変数が設定されていない場合
        requests.HTTPError: トークン取得APIがエラーを返した場合
    """
    print("-> SP-APIアクセストークンを取得中...")
    
    # 環境変数から認証情報を取得
    client_id = os.environ.get("SP_API_CLIENT_ID")
    client_secret = os.environ.get("SP_API_CLIENT_SECRET")
    refresh_token = os.environ.get("SP_API_REFRESH_TOKEN")
    
    # 環境変数の検証
    if not all([client_id, client_secret, refresh_token]):
        raise ValueError("環境変数が正しく設定されていません。SP_API_CLIENT_ID, SP_API_CLIENT_SECRET, SP_API_REFRESH_TOKEN を確認してください。")
    
    # トークン取得APIを呼び出し
    try:
        response = requests.post(
            "https://api.amazon.com/auth/o2/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret
            }
        )
        response.raise_for_status()
        
        access_token = response.json().get("access_token")
        print("-> SP-APIアクセストークンの取得に成功しました。")
        return access_token
        
    except requests.HTTPError as e:
        print(f"Error: SP-APIアクセストークンの取得に失敗しました: {e}")
        print(f"Response: {e.response.text if e.response else 'No response'}")
        raise
    except Exception as e:
        print(f"Error: SP-APIアクセストークンの取得中に予期しないエラーが発生しました: {e}")
        raise
