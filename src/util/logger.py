import sys

def log(message: str, level: str = "INFO"):
    output = f"[secd] [{level}] {message}"
    eprint(output)

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
