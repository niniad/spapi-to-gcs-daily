"""
Settlement Report Backfill Script

このスクリプトは、SP-APIのSettlement Report (GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2) の
過去のレポート一覧を取得し、まだダウンロードされていないものを保存します。

取得したデータは以下に保存されます:
- backfill/data/settlement-report/
"""

import json
import time
import sys
import gzip
import io
from pathlib import Path
from datetime import datetime, timedelta, timezone

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.http_retry import request_with_retry
from backfill.scripts.auth import get_access_token

# ===================================================================
# 設定
# ===================================================================
MARKETPLACE_ID = "A1VC38T7YXB528"  # 日本
SP_API_ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"
REPORT_TYPE = "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2"
DATA_DIR = Path(__file__).parent.parent / "data" / "settlement-report"

# バックフィル期間（過去2年）
BACKFILL_YEARS = 2


def _format_date_for_filename(iso_datetime_str):
    """
    ISO形式の日時文字列をファイル名用の日付形式(YYYYMMDD)に変換します。
    """
    try:
        # ISO形式をパース
        dt = datetime.fromisoformat(iso_datetime_str.replace('Z', '+00:00'))
        return dt.strftime('%Y%m%d')
    except Exception as e:
        print(f"  -> Warn: 日付変換エラー ({iso_datetime_str}): {e}")
        return iso_datetime_str[:10].replace('-', '')


def _generate_filename(report):
    """
    レポート情報からファイル名を生成します。
    Format: settlement-report-data-flat-file-v2-STARTDATE-ENDDATE.tsv
    """
    start_date = _format_date_for_filename(report['dataStartTime'])
    end_date = _format_date_for_filename(report['dataEndTime'])
    return f"settlement-report-data-flat-file-v2-{start_date}-{end_date}.tsv"


def backfill_settlement_report():
    """Settlement Reportのバックフィルを実行します。"""
    print("\n=== Settlement Report バックフィル開始 ===")
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    access_token = get_access_token()
    headers = {
        'Content-Type': 'application/json',
        'x-amz-access-token': access_token
    }
    
    # 過去2年分のレポートを検索するための日付計算
    utc_now = datetime.now(timezone.utc)
    created_since = utc_now - timedelta(days=365 * BACKFILL_YEARS)
    # マイクロ秒を削除し、+00:00をZに置換して厳密なISO8601形式にする
    created_since_str = created_since.replace(microsecond=0).isoformat().replace('+00:00', 'Z')
    
    print(f"-> {created_since_str} 以降に作成されたレポートを検索中...")
    
    # レポート一覧を取得
    params = {
        'reportTypes': REPORT_TYPE,  # リストではなく単一文字列またはカンマ区切りで渡す
        'marketplaceIds': MARKETPLACE_ID, # リストではなく単一文字列で渡す
        'createdSince': created_since_str,
        'pageSize': 100
    }
    
    all_reports = []
    next_token = None
    
    while True:
        if next_token:
            params['nextToken'] = next_token
            
        try:
            response = request_with_retry(
                'GET',
                f"{SP_API_ENDPOINT}/reports/2021-06-30/reports",
                headers=headers,
                params=params
            )
            data = response.json()
            reports = data.get('reports', [])
            all_reports.extend(reports)
            
            next_token = data.get('nextToken')
            if not next_token:
                break
                
            print(f"  ...次のページを取得中 ({len(all_reports)}件取得済)")
            
        except Exception as e:
            # 400エラー等の場合にレスポンス本文を表示する
            print(f"Error: レポート一覧取得失敗: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response Body: {e.response.text}")
            break
            
    print(f"-> レポート一覧取得完了: {len(all_reports)}件")
    
    # DONEかつ未キャンセルのレポートのみフィルタリング
    valid_reports = [r for r in all_reports if r.get('processingStatus') == 'DONE' and r.get('reportDocumentId')]
    print(f"-> 有効なレポート(DONE): {len(valid_reports)}件")
    
    success_count = 0
    skip_count = 0
    
    for report in valid_reports:
        filename = _generate_filename(report)
        filepath = DATA_DIR / filename
        
        if filepath.exists():
            print(f"  [SKIP] {filename} (既存)")
            skip_count += 1
            continue
            
        print(f"  [取得中] {filename}")
        
        try:
            report_document_id = report.get('reportDocumentId')
            
            # ドキュメントURL取得
            get_doc_url = f"{SP_API_ENDPOINT}/reports/2021-06-30/documents/{report_document_id}"
            response = request_with_retry('GET', get_doc_url, headers=headers)
            download_url = response.json()["url"]
            
            # ダウンロード
            response = request_with_retry('GET', download_url)
            
            # 解凍処理
            content = None
            try:
                with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                    content = f.read()
            except gzip.BadGzipFile:
                # TSVなのでエンコーディング注意(通常UTF-8だがcp932等の可能性も考慮)
                try:
                    content = response.content.decode('utf-8')
                except UnicodeDecodeError:
                     content = response.content.decode('cp932', errors='replace')
            
            if content:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"    ✓ 保存完了")
                success_count += 1
            else:
                print(f"    ! Warn: コンテンツが空でした")
        
        except Exception as e:
            print(f"    ! Error: ダウンロード失敗: {e}")
            pass
            
        time.sleep(1)
        
    print(f"\nSettlement Report バックフィル完了: 成功 {success_count}件, スキップ {skip_count}件")


if __name__ == "__main__":
    try:
        backfill_settlement_report()
    except KeyboardInterrupt:
        print("\n中断されました")
    except Exception as e:
        print(f"\nエラー: {e}")
        import traceback
        traceback.print_exc()
