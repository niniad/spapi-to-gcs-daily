import json
from pathlib import Path
import sys

# 標準出力をファイルにリダイレクト（UTF-8）
sys.stdout = open('comparison_result.txt', 'w', encoding='utf-8')
sys.stderr = sys.stdout

file_a = Path(r'c:\Users\ninni\Documents\projects\supabase_test_project\spapi-to-gcs-daily\backfill\data\brand-analytics\WEEK\20251012-20251018.json')
file_b = Path(r'c:\Users\ninni\Documents\projects\supabase_test_project\spapi-to-gcs-daily\backfill\data\brand-analytics\WEEK\20251019-20251025.json')

def get_asins(filepath):
    asins = set()
    if not filepath.exists():
        print(f'File not found: {filepath}')
        return asins
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        data = json.loads(line)
                        asin = data.get('asin')
                        if asin:
                            asins.add(asin)
                    except Exception as e:
                        print(f"Error parsing line in {filepath.name}: {e}")
    except Exception as e:
        print(f"Error opening file {filepath}: {e}")
    return asins

try:
    asins_a = get_asins(file_a)
    asins_b = get_asins(file_b)

    print(f'File A ({file_a.name}): {len(asins_a)} ASINs')
    print(f'File B ({file_b.name}): {len(asins_b)} ASINs')

    only_in_a = asins_a - asins_b
    only_in_b = asins_b - asins_a

    if only_in_a:
        print(f'\nOnly in {file_a.name} ({len(only_in_a)}):')
        print(sorted(list(only_in_a)))

    if only_in_b:
        print(f'\nOnly in {file_b.name} ({len(only_in_b)}):')
        print(sorted(list(only_in_b)))

    if not only_in_a and not only_in_b:
        print('\nASINs match exactly.')

except Exception as e:
    print(f"An error occurred: {e}")
