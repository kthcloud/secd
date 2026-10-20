from kubernetes import client, config
import base64
from app.src.util.setup import get_settings
from app.src.util.logger import log
from typing import Optional

class SecretService():
    def __init__(self, config: client.Configuration):   
        api_client = client.ApiClient(configuration=config)
        self.v1 = client.CoreV1Api(api_client=api_client)

    def get_secret(self, namespace: str, secret_name: str, key: str) -> Optional[str]:
        try:
            secret = self.v1.read_namespaced_secret(secret_name, namespace)
            return base64.b64decode(secret.data[key]).decode('utf-8')
        except client.ApiException as e:
            log(f"Failed to get secret {secret_name} in namespace {namespace}: {e}", "ERROR")
            return None