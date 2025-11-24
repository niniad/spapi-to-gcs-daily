import json
import sys

def count_asins(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        asins = data.get('salesAndTrafficByAsin', [])
        count = len(asins)
        print(f"File: {file_path}")
        print(f"  ASIN Count: {count}")
        if count > 0:
            print(f"  ASINs: {[item.get('parentAsin') for item in asins]}")
            
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python count_asins.py <file1> <file2> ...")
        sys.exit(1)

    for file_path in sys.argv[1:]:
        count_asins(file_path)
