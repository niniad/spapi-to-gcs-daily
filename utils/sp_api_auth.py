"""
SP-API Authentication Utility

このモジュールは、SP-APIのアクセストークンを取得する共通機能を提供します。
全てのSP-APIエンドポイントで使用されます。
"""

import os
import time
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
    
    # Manual .env loader (Fallback for local execution)
    if not os.environ.get("SP_API_REFRESH_TOKEN"):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        env_path = os.path.join(current_dir, '../../.env')
        if os.path.exists(env_path):
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#') or '=' not in line: continue
                    key, value = line.split('=', 1)
                    key = key.strip()
                    if (key.startswith('"') and key.endswith('"')) or (key.startswith("'") and key.endswith("'")): key = key[1:-1]
                    value = value.strip()
                    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")): value = value[1:-1]
                    os.environ[key] = value

    # 環境変数から認証情報を取得
    client_id = os.environ.get("SP_API_CLIENT_ID")
    client_secret = os.environ.get("SP_API_CLIENT_SECRET")
    refresh_token = os.environ.get("SP_API_REFRESH_TOKEN")
    
    # 環境変数の検証
    if not all([client_id, client_secret, refresh_token]):
        raise ValueError("環境変数が正しく設定されていません。SP_API_CLIENT_ID, SP_API_CLIENT_SECRET, SP_API_REFRESH_TOKEN を確認してください。")
    
    # トークン取得APIを呼び出しw
    max_retries = 5
    retry_delay = 1.0

    for attempt in range(max_retries):
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
            
        except requests.exceptions.ConnectionError as e:
            if attempt < max_retries - 1:
                print(f"Warn: SP-APIアクセストークン取得時に接続エラーが発生しました ({e})。{retry_delay}秒後にリトライします... (試行 {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                retry_delay *= 2
                continue
            else:
                print(f"Error: SP-APIアクセストークンの取得に失敗しました (最大リトライ回数超過): {e}")
                raise
        except requests.HTTPError as e:
            # 500系エラーはリトライする
            if attempt < max_retries - 1 and e.response is not None and 500 <= e.response.status_code < 600:
                print(f"Warn: SP-APIサーバーエラー ({e.response.status_code})。{retry_delay}秒後にリトライします... (試行 {attempt + 1}/{max_retries})")
                print(f"Response: {e.response.text}")
                time.sleep(retry_delay)
                retry_delay *= 2
                continue
            
            print(f"Error: SP-APIアクセストークンの取得に失敗しました: {e}")
            print(f"Response: {e.response.text if e.response else 'No response'}")
            raise
        except Exception as e:
            print(f"Error: SP-APIアクセストークンの取得中に予期しないエラーが発生しました: {e}")
            raise


def get_restricted_data_token(path, method='GET', data_elements=None):
    """
    指定されたリソースへのアクセスのためのRestricted Data Token (RDT) を取得します。
    PII（個人情報）へのアクセスに必要です。

    Args:
        path (str): アクセスするAPIのパス (例: '/orders/v0/orders')
        method (str): HTTPメソッド (例: 'GET')
        data_elements (list): 要求するデータ要素のリスト (例: ['buyerInfo', 'shippingAddress'])

    Returns:
        str: Restricted Data Token (RDT)
    """
    print(f"-> RDT (Restricted Data Token) を取得中... (Path: {path})")
    
    # まず通常のアクセストークンを取得
    access_token = get_access_token()
    
    max_retries = 5
    retry_delay = 1.0

    for attempt in range(max_retries):
        try:
            # RDT要求のリクエストボディ作成
            restricted_resources = [
                {
                    "method": method,
                    "path": path,
                    "dataElements": data_elements if data_elements else []
                }
            ]
            
            response = requests.post(
                "https://sellingpartnerapi-fe.amazon.com/tokens/2021-03-01/restrictedDataToken",
                headers={
                    "Content-Type": "application/json",
                    "x-amz-access-token": access_token
                },
                json={
                    "restrictedResources": restricted_resources
                }
            )
            response.raise_for_status()
            
            rdt = response.json().get("restrictedDataToken")
            print("-> RDTの取得に成功しました。")
            return rdt

        except requests.exceptions.ConnectionError as e:
            if attempt < max_retries - 1:
                print(f"Warn: RDT取得時に接続エラーが発生しました ({e})。{retry_delay}秒後にリトライします... (試行 {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                retry_delay *= 2
                continue
            else:
                print(f"Error: RDTの取得に失敗しました (最大リトライ回数超過): {e}")
                raise
        except requests.HTTPError as e:
            # 500系エラーはリトライ
            if attempt < max_retries - 1 and e.response is not None and 500 <= e.response.status_code < 600:
                print(f"Warn: SP-APIサーバーエラー(RDT) ({e.response.status_code})。{retry_delay}秒後にリトライします... (試行 {attempt + 1}/{max_retries})")
                print(f"Response: {e.response.text}")
                time.sleep(retry_delay)
                retry_delay *= 2
                continue

            print(f"Error: RDTの取得に失敗しました: {e}")
            print(f"Response: {e.response.text if e.response else 'No response'}")
            raise
        except Exception as e:
            print(f"Error: RDTの取得中に予期しないエラーが発生しました: {e}")
            raise
