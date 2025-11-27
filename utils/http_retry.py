"""
HTTP Request Retry Utility

SP-APIのレート制限(429エラー)に対応するためのリトライロジックを提供します。
"""

import time
import requests


def request_with_retry(method, url, max_retries=3, retry_delay=60, **kwargs):
    """
    429エラー時に自動的にリトライするHTTPリクエスト関数
    
    Args:
        method: HTTPメソッド ('GET', 'POST' など)
        url: リクエストURL
        max_retries: 最大リトライ回数 (デフォルト: 3)
        retry_delay: リトライ前の待機時間(秒) (デフォルト: 60)
        **kwargs: requests.request()に渡す追加パラメータ
        
    Returns:
        requests.Response: レスポンスオブジェクト
        
    Raises:
        requests.HTTPError: 最大リトライ回数を超えた場合、または429以外のエラーの場合
    """
    for attempt in range(max_retries):
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response
            
        except requests.HTTPError as e:
            # 429エラー(Rate Limit)の場合のみリトライ
            if e.response is not None and e.response.status_code == 429:
                if attempt < max_retries - 1:  # まだリトライ可能
                    wait_time = retry_delay * (2 ** attempt)
                    print(f"  -> Warn: レート制限エラー(429)が発生しました。{wait_time}秒後にリトライします... (試行 {attempt + 1}/{max_retries})")
                    print(f"     URL: {url}")
                    time.sleep(wait_time)
                    continue
                else:  # 最大リトライ回数に達した
                    print(f"  -> Error: 最大リトライ回数({max_retries})に達しました。")
                    raise
            else:
                # 429以外のエラーはそのまま再スロー
                raise
        
        except Exception as e:
            # その他の例外もそのまま再スロー
            raise
    
    # ここには到達しないはずだが、念のため
    raise Exception("予期しないエラー: リトライループを抜けました")
