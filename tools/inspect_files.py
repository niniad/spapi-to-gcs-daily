import os

files = ['backfill/data/all_orders_report/20251220.tsv', 'orders-api_20251215.jsonl']
for p in files:
    try:
        if not os.path.exists(p):
            print(f"File not found: {p}")
            continue
            
        with open(p, 'rb') as f:
            content = f.read()
            print(f'--- {p} ---')
            print(f'Length: {len(content)}')
            if len(content) > 0:
                print(f'Last 10 bytes: {content[-10:]}')
                
                # Count trailing newlines
                cnt = 0
                idx = len(content) - 1
                while idx >= 0 and content[idx] in [10, 13]: # \n or \r
                    cnt += 1
                    idx -= 1
                print(f'Trailing whitespace bytes: {cnt}')
            else:
                print('Empty file')

    except Exception as e:
        print(f'{p}: {e}')
