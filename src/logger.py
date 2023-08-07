import datetime
import sys

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def log(message: str, level: str = "INFO"):
    date = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    output = f"[secd] [{level}] [{date}] {message}"
    
    eprint(output)