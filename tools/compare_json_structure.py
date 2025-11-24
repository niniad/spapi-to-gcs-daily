import json
import sys

def compare_structure(obj1, obj2, path=""):
    """
    Recursively compares the structure (keys and types) of two JSON objects.
    """
    if type(obj1) != type(obj2):
        print(f"Type mismatch at {path}: {type(obj1)} != {type(obj2)}")
        return False

    if isinstance(obj1, dict):
        keys1 = set(obj1.keys())
        keys2 = set(obj2.keys())
        
        if keys1 != keys2:
            missing_in_2 = keys1 - keys2
            missing_in_1 = keys2 - keys1
            if missing_in_2:
                print(f"Keys missing in file 2 at {path}: {missing_in_2}")
            if missing_in_1:
                print(f"Keys missing in file 1 at {path}: {missing_in_1}")
            return False
        
        # Compare values for each key
        match = True
        for key in keys1:
            if not compare_structure(obj1[key], obj2[key], path + "." + key if path else key):
                match = False
        return match

    elif isinstance(obj1, list):
        # For lists, compare the first item if it exists (assuming homogeneous lists)
        if len(obj1) > 0 and len(obj2) > 0:
            return compare_structure(obj1[0], obj2[0], path + "[0]")
        elif len(obj1) > 0 and len(obj2) == 0:
             print(f"List empty in file 2 at {path}")
             return False
        elif len(obj1) == 0 and len(obj2) > 0:
             print(f"List empty in file 1 at {path}")
             return False
        return True

    else:
        # Primitives (int, str, bool, None) - types already checked
        return True

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python compare_json_structure.py <file1> <file2>")
        sys.exit(1)

    file1_path = sys.argv[1]
    file2_path = sys.argv[2]

    try:
        with open(file1_path, 'r', encoding='utf-8') as f1:
            json1 = json.load(f1)
        with open(file2_path, 'r', encoding='utf-8') as f2:
            json2 = json.load(f2)

        print(f"Comparing {file1_path} and {file2_path}...")
        if compare_structure(json1, json2):
            print("Structure matches perfectly!")
        else:
            print("Structure differences found.")

    except Exception as e:
        print(f"Error: {e}")
