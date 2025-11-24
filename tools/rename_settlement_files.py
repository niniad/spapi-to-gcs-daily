"""
Settlement Report File Renaming Script

このスクリプトは、temp/settlement_report_data内のファイルを読み取り、
ファイル内容から日付範囲を抽出して、新しい命名規則に従ってリネームします。

旧形式: 64794019933.txt (reportIdのみ)
新形式: 20240715-20240729.tsv (開始日-終了日)
"""

import os
import re
from pathlib import Path

def extract_dates_from_file(file_path):
    """
    ファイルの最初の2行から日付範囲を抽出します。
    
    Args:
        file_path: ファイルパス
        
    Returns:
        tuple: (start_date, end_date) YYYYMMDD形式、または None
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # ヘッダー行をスキップ
            header = f.readline()
            # データ行を読む
            data_line = f.readline()
            
            if not data_line.strip():
                return None
            
            # タブ区切りで分割
            fields = data_line.split('\t')
            
            # settlement-start-date (index 1) と settlement-end-date (index 2)
            if len(fields) < 3:
                return None
            
            start_date_str = fields[1].strip()  # "2024/07/15 11:06:18 UTC"
            end_date_str = fields[2].strip()    # "2024/07/29 11:06:18 UTC"
            
            # 日付部分を抽出 (YYYY/MM/DD)
            start_match = re.match(r'(\d{4})/(\d{2})/(\d{2})', start_date_str)
            end_match = re.match(r'(\d{4})/(\d{2})/(\d{2})', end_date_str)
            
            if start_match and end_match:
                start_date = f"{start_match.group(1)}{start_match.group(2)}{start_match.group(3)}"
                end_date = f"{end_match.group(1)}{end_match.group(2)}{end_match.group(3)}"
                return (start_date, end_date)
            
            return None
    
    except Exception as e:
        print(f"  Error reading {file_path}: {e}")
        return None


def rename_settlement_files(source_dir, dry_run=True):
    """
    Settlement reportファイルをリネームします。
    
    Args:
        source_dir: ソースディレクトリ
        dry_run: Trueの場合、実際にはリネームせず、プレビューのみ
    """
    source_path = Path(source_dir)
    
    if not source_path.exists():
        print(f"Error: ディレクトリが存在しません: {source_dir}")
        return
    
    print(f"ディレクトリ: {source_dir}")
    if dry_run:
        print("[DRY RUN] 実際にはリネームしません。\n")
    
    files = list(source_path.glob("*.txt"))
    print(f"対象ファイル数: {len(files)}\n")
    
    renamed_count = 0
    skipped_count = 0
    
    for file_path in files:
        dates = extract_dates_from_file(file_path)
        
        if dates:
            start_date, end_date = dates
            new_name = f"{start_date}-{end_date}.tsv"
            new_path = file_path.parent / new_name
            
            print(f"{file_path.name} -> {new_name}")
            
            if not dry_run:
                try:
                    file_path.rename(new_path)
                    renamed_count += 1
                except Exception as e:
                    print(f"  Error: リネーム失敗: {e}")
                    skipped_count += 1
            else:
                renamed_count += 1
        else:
            print(f"[SKIP] {file_path.name} (日付抽出失敗)")
            skipped_count += 1
    
    print(f"\n{'[DRY RUN] ' if dry_run else ''}完了:")
    print(f"  リネーム: {renamed_count}件")
    print(f"  スキップ: {skipped_count}件")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Settlement reportファイルをリネームします。")
    parser.add_argument("--execute", action="store_true", help="実際にリネームを実行します")
    args = parser.parse_args()
    
    source_dir = "temp/settlement_report_data"
    rename_settlement_files(source_dir, dry_run=not args.execute)
