"""
SP-API Authentication Module for Backfill Scripts

このモジュールは、バックフィルスクリプト用のSP-API認証を提供します。
認証情報は backfill/config/credentials.json から読み込みます。
"""

import json
import requests
from pathlib import Path


def load_credentials():
    """
    認証情報ファイルを読み込みます。
    
    Returns:
        dict: 認証情報 (refresh_token, client_id, client_secret)
    
    Raises:
        FileNotFoundError: 認証情報ファイルが見つからない場合
        json.JSONDecodeError: JSONのパースに失敗した場合
    """
    credentials_path = Path(__file__).parent.parent / "config" / "credentials.json"
    
    if not credentials_path.exists():
        raise FileNotFoundError(
            f"認証情報ファイルが見つかりません: {credentials_path}\n"
            "backfill/config/credentials.json を作成してください。\n"
            "形式:\n"
            "{\n"
            '  "refresh_token": "YOUR_REFRESH_TOKEN",\n'
            '  "client_id": "YOUR_CLIENT_ID",\n'
            '  "client_secret": "YOUR_CLIENT_SECRET"\n'
            "}"
        )
    
    with open(credentials_path, 'r', encoding='utf-8') as f:
        credentials = json.load(f)
    
    required_keys = ['refresh_token', 'client_id', 'client_secret']
    for key in required_keys:
        if key not in credentials:
            raise ValueError(f"認証情報ファイルに '{key}' が含まれていません。")
    
    return credentials


def get_access_token():
    """
    SP-APIアクセストークンを取得します。
    
    Returns:
        str: アクセストークン
    
    Raises:
        requests.exceptions.RequestException: API呼び出しに失敗した場合
    """
    credentials = load_credentials()
    
    token_url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": credentials['refresh_token'],
        "client_id": credentials['client_id'],
        "client_secret": credentials['client_secret']
    }
    
    response = requests.post(token_url, data=payload)
    response.raise_for_status()
    
    return response.json()["access_token"]


if __name__ == "__main__":
    # テスト実行
    try:
        print("認証情報を読み込んでいます...")
        credentials = load_credentials()
        print(f"✓ 認証情報の読み込み成功")
        print(f"  Client ID: {credentials['client_id'][:10]}...")
        
        print("\nアクセストークンを取得しています...")
        token = get_access_token()
        print(f"✓ アクセストークンの取得成功")
        print(f"  Token: {token[:20]}...")
        
    except Exception as e:
        print(f"✗ エラー: {e}")
