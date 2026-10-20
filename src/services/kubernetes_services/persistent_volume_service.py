import datetime
import time
from kubernetes import client, config
from app.src.util.setup import get_settings
from app.src.util.logger import log
from typing import List, Optional

class PersistentVolumeService():
    def __init__(self, config: client.Configuration):   
        api_client = client.ApiClient(configuration=config)
        self.v1 = client.CoreV1Api(api_client)

    def create_persistent_volume(
        self,
        name: str,
        path: str,
        capacity: str = "50Gi",
        access_modes: List[str] = ["ReadWriteOnce"]
    ) -> client.V1PersistentVolume:
        pv = client.V1PersistentVolume(
            metadata=client.V1ObjectMeta(name=name),
            spec=client.V1PersistentVolumeSpec(
                access_modes=access_modes,
                capacity={"storage": capacity},
                nfs=client.V1NFSVolumeSource(
                    path=path, 
                    server="nfs.secd"
                ),
                storage_class_name="nfs",
                persistent_volume_reclaim_policy="Retain",
                volume_mode="Filesystem",
            )
        )
        try:
            self.v1.create_persistent_volume(body=pv)
            log(f"{name} PV created")
            return pv
        except client.ApiException as e:
            log(f"Failed to create PV {name}: {e}", "ERROR")
            raise

    def get_persistent_volume(self, name: str) -> Optional[client.V1PersistentVolume]:
        try:
            return self.v1.read_persistent_volume(name)
        except client.ApiException as e:
            log(f"Failed to get PV {name}: {e}", "ERROR")
            return None
    
    def get_pvc(self, name: str, namespace: str) -> Optional[client.V1PersistentVolumeClaim]:
        """Retrieve a Persistent Volume Claim by name and namespace.
        Args:
            name (str): The name of the PVC (e.g., "pvc-storage-mysql-1").
            namespace (str): The namespace where the PVC resides (e.g., "storage").

        Returns:
            Optional[client.V1PersistentVolumeClaim]: The PVC object if found, None otherwise.
        """
        try:
            pvc = self.v1.read_namespaced_persistent_volume_claim(
                name=name,
                namespace=namespace
            )
            return pvc
        except client.ApiException as e:
            if e.status == 404:
                log(f"PVC '{name}' not found in namespace '{namespace}'", "WARNING")
            else:
                log(f"Failed to get PVC '{name}' in namespace '{namespace}': {str(e)}", "ERROR")
            return None

    def delete_persistent_volume(self, name: str) -> None:
        try:
            self.v1.delete_persistent_volume(name)
            log(f"PV {name} deleted")
        except client.ApiException as e:
            log(f"Failed to delete PV {name}: {e}", "ERROR")

    def create_persistent_volume_claim(
        self,
        pvc_name: str,
        namespace: str,
        volume_name: str,
        storage_size: str = "100Gi",
        access_modes: Optional[List[str]] = None,
        storage_class_name: str = "nfs"
    ) -> client.V1PersistentVolumeClaim:
        if access_modes is None:
            access_modes = ["ReadOnlyMany"]
        pvc = client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(name=pvc_name, namespace=namespace),
            spec=client.V1PersistentVolumeClaimSpec(
                access_modes=access_modes,
                resources=client.V1ResourceRequirements(requests={"storage": storage_size}),
                storage_class_name=storage_class_name,
                volume_name=volume_name
            )
        )
        try:
            self.v1.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc)
            log(f"{pvc_name} PVC created in namespace {namespace}")
            return pvc
        except client.ApiException as e:
            log(f"Failed to create PVC {pvc_name}: {e}", "ERROR")
            raise

    def get_persistent_volume_claim(self, namespace: str, name: str) -> Optional[client.V1PersistentVolumeClaim]:
        try:
            return self.v1.read_namespaced_persistent_volume_claim(name, namespace)
        except client.ApiException as e:
            log(f"Failed to get PVC {name} in namespace {namespace}: {e}", "ERROR")
            return None

    def delete_persistent_volume_claim(self, namespace: str, name: str) -> None:
        try:
            self.v1.delete_namespaced_persistent_volume_claim(name, namespace)
            log(f"PVC {name} deleted in namespace {namespace}")
        except client.ApiException as e:
            log(f"Failed to delete PVC {name} in namespace {namespace}: {e}", "ERROR")

    def cleanup_persistent_volumes(self, namespaces: List[client.V1Namespace]) -> None:
        try:
            for namespace in namespaces:
                namespace_name = namespace.metadata.name
                if self._should_cleanup_namespace(namespace):
                    pvc_names = self._get_pvc_names(namespace_name)
                    log(f"Cleaning up {len(pvc_names)} PVCs in namespace {namespace_name}")
                    for pvc_name in pvc_names:
                        pv_names = self._get_pv_names_from_namespace(namespace_name)
                        log(f"pv_names: {pv_names}")
                        self.delete_persistent_volume_claim(namespace_name, pvc_name)
                        self._wait_for_pvc_deletion(namespace_name, pvc_names)
                        log(f"Cleaning up {len(pv_names)} PVs in namespace {namespace_name}")
                        self._make_pv_available(pv_names)
        except client.ApiException as e:
            log(f"Failed to cleanup PVs: {e}", "ERROR")

    def get_pv_by_helm_release(self, release_name: str) -> Optional[client.V1PersistentVolume]:
        try:
            pvs = self.v1.list_persistent_volume(label_selector=f"release={release_name}").items
            if pvs:
                log(f"Found PV for release '{release_name}': {pvs[0].metadata.name}")
                return pvs[0]
            return None
        except client.ApiException as e:
            log(f"Failed to list PVs with label release={release_name}: {e}", "ERROR")
            

    # Helper Methods
    def _get_pvc_names(self, namespace_name: str) -> List[str]:
        try:
            pvc_list = self.v1.list_namespaced_persistent_volume_claim(namespace=namespace_name)
            return [pvc.metadata.name for pvc in pvc_list.items]
        except client.ApiException as e:
            log(f"Failed to list PVCs in namespace {namespace_name}: {e}", "ERROR")
            return []

    def _get_pv_names_from_namespace(self, namespace_name: str) -> List[str]:
        pv_names = []
        try:
            pvc_list = self.v1.list_namespaced_persistent_volume_claim(namespace=namespace_name)
            for pvc in pvc_list.items:
                if pvc.spec.volume_name:
                    pv_names.append(pvc.spec.volume_name)
        except client.ApiException as e:
            log(f"Failed to list PVCs in namespace {namespace_name}: {e}", "ERROR")
        return pv_names

    def _wait_for_pvc_deletion(self, namespace_name: str, pvc_names: List[str], timeout: int = 60) -> None:
        start_time = time.time()
        while time.time() - start_time < timeout:
            remaining = False
            for pvc_name in pvc_names:
                try:
                    self.v1.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace_name)
                    remaining = True
                except client.ApiException as e:
                    if e.status == 404:
                        log(f"PVC {pvc_name} deleted in namespace {namespace_name}")
                    else:
                        log(f"Error checking PVC {pvc_name}: {e}", "ERROR")
            if not remaining:
                return
            time.sleep(5)
        log(f"Timeout waiting for PVC deletion in namespace {namespace_name}", "WARNING")

    def _make_pv_available(self, pv_names: List[str]) -> None:
        for pv_name in pv_names:
            try:
                pv = self.v1.read_persistent_volume(pv_name)
                if pv.status.phase == "Released":
                    self.v1.patch_persistent_volume(pv_name, {"spec": {"claimRef": None}})
                    log(f"PV {pv_name} is now available")
            except client.ApiException as e:
                log(f"Error making PV {pv_name} available: {e}", "ERROR")

    def _should_cleanup_namespace(self, namespace) -> bool:
        annotations = namespace.metadata.annotations or {}
        if 'rununtil' not in annotations:
            return False
        expired = datetime.datetime.fromisoformat(annotations['rununtil']) < datetime.datetime.now()
        completed = self._is_pod_completed(namespace.metadata.name)
        return expired or completed
    
    def _is_pod_completed(self, namespace_name: str) -> bool:
        pod_list = self.v1.list_namespaced_pod(namespace=namespace_name)
        if len(pod_list.items) > 0:
            pod = pod_list.items[0]
            log(f"Pod found in namespace: {namespace_name} with phase: {pod.status.phase}")
            return pod.status.phase in ['Succeeded', 'Failed']
        return False
