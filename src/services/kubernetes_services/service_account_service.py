import datetime
from typing import List
from kubernetes import client
from app.src.util.logger import log

class ServiceAccountService():
    def __init__(self, config: client.Configuration):   
        api_client = client.ApiClient(configuration=config)
        self.v1 = client.CoreV1Api(api_client=api_client)

    def create_service_account(self, name: str, namespace: str) -> None:
        sa = client.V1ServiceAccount(
            metadata=client.V1ObjectMeta(name=name, namespace=namespace)
        )
        self.v1.create_namespaced_service_account(namespace=namespace, body=sa)

    def delete_service_account(self, name: str, namespace: str) -> None:    
        self.v1.delete_namespaced_service_account(name=name, namespace=namespace)
    
    def cleanup_service_accounts(self, namespaces: List[client.V1Namespace]) -> None:
        for namespace in namespaces:
            namespace_name = namespace.metadata.name
            if self._should_cleanup_namespace(namespace):
                sa_list = self.v1.list_namespaced_service_account(namespace=namespace_name)
                for sa in sa_list.items:
                    if sa.metadata.name != "default":  # Skip the default service account
                        self.delete_service_account(sa.metadata.name, namespace_name)

    # Helper methods
    def _should_cleanup_namespace(self, namespace) -> bool:
        """Determine if the namespace should be cleaned up based on annotations or pod status."""
        annotations = namespace.metadata.annotations or {}
        if 'rununtil' not in annotations:
            return False
        expired = datetime.datetime.fromisoformat(annotations['rununtil']) < datetime.datetime.now()
        completed = self._is_pod_completed(namespace.metadata.name)
        return expired or completed

    def _is_pod_completed(self, namespace_name: str) -> bool:
        """Check if the pod in the namespace is completed (Succeeded or Failed)."""
        pod_list = self.v1.list_namespaced_pod(namespace=namespace_name)
        if len(pod_list.items) > 0:
            pod = pod_list.items[0]  # Assuming one pod per namespace, as in pv_service
            #log(f"Pod found in namespace: {namespace_name} with phase: {pod.status.phase}", "DEBUG")
            return pod.status.phase in ['Succeeded', 'Failed']
        return False  # No pods means we can proceed with cleanup