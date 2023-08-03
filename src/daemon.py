import time
import src.k8s_service as k8s_service
import src.gitlab_service as gitlab_service
from src.logger import log

def run():
    log("Starting daemon...")
    while True:
        cleaned_run_ids = k8s_service.cleanup_resources()
        for run_id in cleaned_run_ids:
            log(f"Finishing run {run_id} - expired rununtil - Pushing results")
            gitlab_service.push_results(run_id)
            log(f"Finishing run {run_id} - Finished and cleaned up")
        
        time.sleep(60)