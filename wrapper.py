
import sys
import traceback

try:
    import verify_startup
except Exception:
    with open("wrapper_error.log", "w") as f:
        traceback.print_exc(file=f)
