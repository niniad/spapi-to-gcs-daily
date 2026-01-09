
import sys
import os
import traceback

with open("startup_status.txt", "w") as f:
    f.write("Starting verification...\n")

try:
    sys.path.append(os.getcwd())
    import main
    with open("startup_status.txt", "a") as f:
        f.write("Successfully imported main.\n")
except Exception:
    with open("startup_status.txt", "a") as f:
        f.write("Failed to import main:\n")
        traceback.print_exc(file=f)
    sys.exit(1)
