"""
HTTP Request Retry Utility

SP-APIのレート制限(429エラー)に対応するためのリトライロジックを提供します。
"""

import time
import requests


def request_with_retry(method, url, max_retries=4, **kwargs):
    """
    429エラー時に自動的にリトライするHTTPリクエスト関数
    
    Args:
        method: HTTPメソッド ('GET', 'POST' など)
        url: リクエストURL
        max_retries: 最大リトライ回数 (デフォルト: 4, 初回 + リトライ3回)
        **kwargs: requests.request()に渡す追加パラメータ
                  retry_delays (list[int]): リトライ試行ごとの待機時間リスト(秒) (例: [60, 300, 300])。
                  指定がない場合は [60, 300, 300] が使用されます。
        
    Returns:
        requests.Response: レスポンスオブジェクト
        
    Raises:
        requests.HTTPError: 最大リトライ回数を超えた場合、または429以外のエラーの場合
    """
    # kwargsからリトライ設定を取り出す（requestsには渡さない）
    # 後方互換性のため retry_delay があれば無視する
    kwargs.pop('retry_delay', None)
    
    retry_delays = kwargs.pop('retry_delays', [60, 300, 300])

    for attempt in range(max_retries):
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response
            
        except requests.HTTPError as e:
            # 429エラー(Rate Limit)の場合のみリトライ
            if e.response is not None and e.response.status_code == 429:
                if attempt < len(retry_delays):
                    wait_time = retry_delays[attempt]
                else:
                    # リスト設定以上の回数の場合は最後の値を使用するか、デフォルト動作に倒す
                    # ここではリストの最後の値を使用する
                    wait_time = retry_delays[-1]

                if attempt < max_retries - 1:  # まだリトライ可能
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
