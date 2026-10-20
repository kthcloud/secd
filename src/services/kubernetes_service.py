from typing import Optional, Dict, List
from kubernetes import client
from app.src.util.setup import get_settings
from app.src.util.logger import log
from app.src.services.kubernetes_services.namespace_service import NamespaceService
from app.src.services.kubernetes_services.pod_service import PodService
from app.src.services.kubernetes_services.persistent_volume_service import PersistentVolumeService
from app.src.services.kubernetes_services.secret_service import SecretService
from app.src.services.kubernetes_services.helm_service import HelmService
from app.src.services.kubernetes_services.service_account_service import ServiceAccountService

import os
import datetime

class KubernetesService:
    def __init__(
        self,
        namespace_service: NamespaceService,
        pod_service: PodService,
        pv_service: PersistentVolumeService,
        secret_service: SecretService,
        helm_service: HelmService,
        service_account_service: ServiceAccountService
    ):
        self.namespace_service = namespace_service
        self.pod_service = pod_service
        self.pv_service = pv_service
        self.secret_service = secret_service
        self.helm_service = helm_service
        self.service_account_service = service_account_service
        self.config_path = get_settings()['k8s']['configPath']

    def handle_cache_dir(self, run_meta: Dict, keycloak_user_id: str, run_id: str) -> tuple[Optional[str], Optional[str]]:
        cache_dir = None
        mount_path = None
        if "cache_dir" in run_meta and run_meta["cache_dir"]:
            mount_path = run_meta.get('mount_path', '/cache')
            cache_dir = run_meta['cache_dir']
            cache_path = f"{get_settings()['path']['cachePath']}/{keycloak_user_id}/{cache_dir}"
            if not os.path.exists(cache_path):
                os.makedirs(cache_path)
                log(f"Cache directory created at: {cache_path}")
        return cache_dir, mount_path

    def create_namespace(self, user_id: str, run_id: str, run_for: int, labels: str) -> None:
        run_until = datetime.datetime.now() + datetime.timedelta(hours=run_for)
        namespace_name = f"secd-{run_id}"
        annotations = {"userid": user_id, "rununtil": run_until.isoformat()}
        self.namespace_service.create_namespace(namespace_name, labels, annotations)

    def cleanup_resources(self) -> List[str]:
        secd_namespaces = []

        namespaces = self.namespace_service.get_namespaces()
        for namespace in namespaces.items:
            if namespace.metadata.name.startswith("secd-"):
                secd_namespaces.append(namespace)

        self.pv_service.cleanup_persistent_volumes(secd_namespaces)
        self.service_account_service.cleanup_service_accounts(secd_namespaces)

        run_ids = self.namespace_service.cleanup_namespaces(secd_namespaces)
        return run_ids

    def get_secret(self, namespace: str, secret_name: str, key: str) -> Optional[str]:
        return self.secret_service.get_secret(namespace, secret_name, key)

    def get_pod_by_helm_release(self, release_name: str, namespace: str) -> Optional[client.V1Pod]:
        return self.pod_service.get_pod_by_helm_release(release_name, namespace)

    def get_service_by_helm_release(self, release_name: str, namespace: str) -> Optional[str]:
        return self.helm_service.get_service_by_helm_release(release_name, namespace)

    def create_service_account(self, name: str, namespace: str) -> None:
        return self.service_account_service.create_service_account(name, namespace)
        