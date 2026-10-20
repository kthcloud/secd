from kubernetes import client
from app.src.util.logger import log
from typing import Optional

class HelmService():
    def __init__(self, config: client.Configuration):   
        api_client = client.ApiClient(configuration=config)
        self.v1 = client.CoreV1Api(api_client=api_client)

    def get_service_by_helm_release(self, release_name: str, namespace: str) -> Optional[str]:
        services = self.v1.list_namespaced_service(namespace).items
        for service in services:
            labels = service.metadata.labels or {}
            if labels.get('release') == release_name:
                service_fqdn = f"{service.metadata.name}.{namespace}.svc.cluster.local"
                log(f"Service '{service.metadata.name}' matches Helm release '{release_name}'.")
                return service_fqdn
        return None