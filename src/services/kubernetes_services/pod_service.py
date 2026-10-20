from kubernetes import client
from app.src.util.setup import get_settings
from app.src.util.logger import log
from typing import List, Optional, Dict

class PodService():
    def __init__(self, config: client.Configuration):   
        api_client = client.ApiClient(configuration=config)
        self.v1 = client.CoreV1Api(api_client=api_client)
    
    def get_pod_by_label(self, label_selector: str, namespace: str) -> Optional[client.V1Pod]:
        try:
            pods = self.v1.list_namespaced_pod(
                namespace=namespace,
                label_selector=label_selector
            ).items
            if pods:
                pod = pods[0]
                #log(f"Pod '{pod.metadata.name}' found with label selector '{label_selector}' in namespace '{namespace}'", "DEBUG")
                return pod
            log(f"No pod found with label selector '{label_selector}' in namespace '{namespace}'", "WARNING")
            return None
        except client.ApiException as e:
            log(f"Failed to get pod with label selector '{label_selector}' in namespace '{namespace}': {str(e)}", "ERROR")
            return None
        
    def create_nfs_pod(
            self, 
            database_name,
            run_id,
            image_name,
            environment_variables,
        ):

        pod_name = f"secd-{run_id}"
        
        # Define the NFS-backed volume and output volume
        persistence_volumes = [
            client.V1PersistentVolume(
                metadata=client.V1ObjectMeta(
                    name=f"secd-pv-{run_id}-input"
                ),
                spec=client.V1PersistentVolumeSpec(
                    access_modes = ["ReadOnlyMany"],
                    capacity={"storage" : "25Gi"},
                    nfs=client.V1NFSVolumeSource(
                        path=f"/mnt/cloud/apps/sec/storage/{database_name}",
                        server="nfs.secd",
                        read_only=True
                    ),
                    storage_class_name="nfs",
                    persistent_volume_reclaim_policy="Retain",
                    volume_mode="Filesystem",
                )
            ),
        ]

        pod_volumes = [
            client.V1Volume(
                name=f"secd-pv-{run_id}-input",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=f"secd-pvc-{run_id}-input",
                    read_only=True
                )
            ),
            client.V1Volume(
                name=f"secd-pv-{run_id}-output",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=f"secd-pvc-{run_id}-output",
                )
            ),
        ]

        # PVCs
        persistence_volume_claims = [
            client.V1PersistentVolumeClaim(
                metadata=client.V1ObjectMeta(
                    name=f"secd-pvc-{run_id}-input",
                    namespace=pod_name
                ),
                spec=client.V1PersistentVolumeClaimSpec(
                    access_modes=["ReadOnlyMany"],
                    resources=client.V1ResourceRequirements(
                        requests={"storage" : "25Gi"}
                    ),
                    storage_class_name="nfs",
                    volume_name=f"secd-pv-{run_id}-input"
                )
            ),
            client.V1PersistentVolumeClaim(
                metadata=client.V1ObjectMeta(
                    name=f"secd-pvc-{run_id}-output",
                    namespace=pod_name
                ),
                spec=client.V1PersistentVolumeClaimSpec(
                    access_modes=["ReadWriteOnce"],
                    resources=client.V1ResourceRequirements(
                        requests={"storage" : "25Gi"}
                    ),
                    storage_class_name="nfs",
                    volume_name=f"secd-pv-{run_id}-output"
                )
            )
        ]

        # Extract environment variables
        k8s_envs = []
        for key, value in environment_variables.items():
            env_var = client.V1EnvVar(
                name=key,
                value=value
            )
            k8s_envs.append(env_var)

        # Define the container with its volume mount and envrionment variables
        containers = [
            client.V1Container(
                name=pod_name,
                image=image_name,
                env=k8s_envs,
                volume_mounts=[
                    client.V1VolumeMount(
                        name=f"secd-pv-{run_id}-input",
                        mount_path="/data",
                        read_only=True
                    ),
                    client.V1VolumeMount(
                        name=f"secd-pv-{run_id}-output",
                        mount_path="/output",
                        read_only=False
                    )
                ]
            )
        ]

        pod = client.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=client.V1ObjectMeta(name=pod_name),
            spec=client.V1PodSpec(
                volumes=pod_volumes,
                containers=containers,
                restart_policy="Never"
            )
        )

        # Initialise everything
        for pv in persistence_volumes:
            self.v1.create_persistent_volume(pv)
        for pvc in persistence_volume_claims:
            self.v1.create_namespaced_persistent_volume_claim(namespace=pod_name, body=pvc)
        
        self.v1.create_namespaced_pod(namespace=pod_name, body=pod)

    def create_pod_by_vault(
        self,
        run_id: str,
        image: str,
        envs: Dict[str, str],
        gpu: str,
        mount_path: Optional[str],
        database: str,
        namespace: str,
        pvc_name: str,
        vault_role: str
    ) -> client.V1Pod:
        try:
            pod_name = f"secd-{run_id}"
            database_name = database 
            service_account_name = f"sa-{database_name}"  # e.g., sa-mysql-1
            k8s_auth_role_name = f"role-{database_name}-{namespace}"  # e.g., role-mysql-1-secd-<run_id>
            db_role_name = f"role-{database_name}"  # e.g., role-mysql-1

            # Vault annotations for credential injection
            annotations = {
                "vault.hashicorp.com/agent-inject": "true",
                "vault.hashicorp.com/role": k8s_auth_role_name,
                "vault.hashicorp.com/agent-inject-secret-dbcreds": f"database/creds/{db_role_name}",
                "vault.hashicorp.com/agent-inject-template-dbcreds": f"""
                {{{{- with secret "database/creds/{db_role_name}" -}}}}
                export DB_USER="{{{{ .Data.username }}}}"
                export DB_PASS="{{{{ .Data.password }}}}"
                {{{{- end -}}}}
                """
            }

            k8s_envs = [client.V1EnvVar(name=key, value=value) for key, value in envs.items()]
            resources = client.V1ResourceRequirements()
            labels = {"name": database_name, "run_id": run_id}
            volumes = []
            volume_mounts = []

            # Output folder
            volumes.append(self._create_volume(f"vol-{run_id}-output", f"secd-pvc-{run_id}-output"))
            volume_mounts.append(self._create_mount(f"vol-{run_id}-output", "/output"))

            # PVC mount for database 
            if pvc_name:
                volumes.append(self._create_volume(pvc_name, pvc_name, read_only=True))
                volume_mounts.append(self._create_mount(pvc_name, "/data", read_only=True))
            
            # Cache
            if mount_path:
                volumes.append(self._create_volume(f"vol-{run_id}-cache", f"secd-pvc-{run_id}-cache"))
                volume_mounts.append(self._create_mount(f"vol-{run_id}-cache", mount_path))
            
            # GPU mount
            if gpu:
                resources, gpu_labels = self._set_resources(gpu=1)
                labels.update(gpu_labels)
            container = self._create_container(
                pod_name, image, k8s_envs, volume_mounts, resources,
                command=["/bin/sh", "-c"],
                args=[". /vault/secrets/dbcreds && env | grep DB_ && python /app/app.py"]
            )
            pod_spec = self._create_pod_spec(volumes, [container], service_account=service_account_name)
            pod = self._create_pod_object(pod_name, labels, pod_spec, annotations=annotations)
            self.v1.create_namespaced_pod(namespace=namespace, body=pod)
            log(f"{pod_name} Pod created in namespace {namespace}")
            return pod
        except Exception as e:
            log(f"Error creating pod with Vault v3: {str(e)}", "ERROR")
            raise Exception(f"Error creating pod with Vault v3: {e}")

    # Remaining methods unchanged (read_namespaced_pod_log, _create_volume, etc.)
    def read_namespaced_pod_log(self, name: str, namespace: str, container: str, exec_command: List[str]) -> str:
        try:
            resp = self.v1.read_namespaced_pod_log(
                name=name,
                namespace=namespace,
                container=container,
                exec_command=exec_command
            )
            #log(f"Read logs from pod {name}", "DEBUG")
            return resp.strip()
        except Exception as e:
            log(f"Error reading logs from pod {name}: {str(e)}", "ERROR")
            raise Exception(f"Error reading logs from pod {name}: {e}")

    def _create_volume(self, name: str, claim_name: str, read_only: bool = False) -> client.V1Volume:
        return client.V1Volume(
            name=name,
            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                claim_name=claim_name,
                read_only=read_only
            )
        )

    def _create_mount(self, name: str, mount_path: str, read_only: bool = False) -> client.V1VolumeMount:
        return client.V1VolumeMount(
            name=name,
            mount_path=mount_path,
            read_only=read_only
        )

    def _set_resources(self, gpu: int) -> tuple[client.V1ResourceRequirements, Dict[str, str]]:
        resources = client.V1ResourceRequirements(
            limits={"nvidia.com/gpu": str(gpu)},
            requests={"nvidia.com/gpu": str(gpu)}
        )
        labels = {"gpu": "true"}
        return resources, labels

    def _create_container(
        self,
        name: str,
        image: str,
        envs: List[client.V1EnvVar],
        mounts: List[client.V1VolumeMount],
        resources: client.V1ResourceRequirements,
        command: Optional[List[str]] = None,
        args: Optional[List[str]] = None
    ) -> client.V1Container:
        return client.V1Container(
            name=name,
            image=image,
            env=envs,
            volume_mounts=mounts,
            resources=resources,
            command=command,
            args=args
        )

    def _create_pod_spec(self, volumes: List[client.V1Volume], containers: List[client.V1Container], service_account: str = "") -> client.V1PodSpec:
        return client.V1PodSpec(
            service_account=service_account,
            volumes=volumes,
            containers=containers,
            restart_policy="Never"
        )

    def _create_pod_object(self, name: str, labels: Dict[str, str], spec: client.V1PodSpec, annotations: Optional[Dict[str, str]] = None) -> client.V1Pod:
        return client.V1Pod(
            metadata=client.V1ObjectMeta(
                name=name, 
                labels=labels, 
                annotations=annotations),
            spec=spec
        )

    def get_pod(self, namespace: str, name: str) -> Optional[client.V1Pod]:
        try:
            return self.v1.read_namespaced_pod(name, namespace)
        except client.ApiException as e:
            log(f"Failed to get pod {name} in namespace {namespace}: {e}", "ERROR")
            return None

    def list_pods(self, namespace: str) -> List[client.V1Pod]:
        return self.v1.list_namespaced_pod(namespace).items

    def delete_pod(self, namespace: str, name: str) -> None:
        try:
            self.v1.delete_namespaced_pod(name, namespace)
            log(f"Pod {name} deleted in namespace {namespace}")
        except client.ApiException as e:
            log(f"Failed to delete pod {name} in namespace {namespace}: {e}", "ERROR")

    def get_pod_by_helm_release(self, release_name: str, namespace: str) -> Optional[client.V1Pod]:
        pods = self.list_pods(namespace)
        for pod in pods:
            labels = pod.metadata.labels or {}
            if labels.get('release') == release_name:
                log(f"Pod '{pod.metadata.name}' matches Helm release '{release_name}'.")
                return pod
        return None

    def get_pod_ip(self, namespace: str, pod_name_prefix: str) -> Optional[str]:
        pods = self.list_pods(namespace)
        for pod in pods:
            if pod_name_prefix in pod.metadata.name:
                return pod.status.pod_ip
        return None