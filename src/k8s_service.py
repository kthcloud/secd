import datetime
from kubernetes import client, config
from typing import Dict, List

from src.setup import get_settings
from src.logger import log


def _with_k8s():
    config.load_kube_config(get_settings()['k8s']['configPath'])
    v1 = client.CoreV1Api()
    return v1


def create_namespace(user_id: str, run_id: str, run_for: datetime):
    v1 = _with_k8s()

    run_until = datetime.datetime.now() + datetime.timedelta(hours=run_for)

    # create namespace if not exists
    namespace = client.V1Namespace()
    namespace.metadata = client.V1ObjectMeta(
        name=f"secd-{run_id}",
        annotations={
            "userid": user_id,
            "rununtil": run_until.isoformat(),
        }
    )
    v1.create_namespace(body=namespace)


def create_pod(run_id: str, image: str, envs: Dict[str, str]):
    v1 = _with_k8s()

    k8s_envs = []
    for env in envs:
        k8s_envs.append(client.V1EnvVar(name=env, value=envs[env]))

    # Create pod
    pod = client.V1Pod()
    pod.metadata = client.V1ObjectMeta(
        name=f"secd-{run_id}",
    )
    pod.spec = client.V1PodSpec(
        volumes=[
            client.V1Volume(
                name=f'vol-{run_id}-output',
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=f'secd-pvc-{run_id}-output'
                )
            )
        ],
        containers=[
            client.V1Container(
                name=f"secd-{run_id}",
                image=image,
                env=k8s_envs,
                volume_mounts=[
                    client.V1VolumeMount(
                        name=f'vol-{run_id}-output',
                        mount_path='/output'
                    )
                ]
            )
        ]
    )
    v1.create_namespaced_pod(namespace=f"secd-{run_id}", body=pod)


def create_persistent_volume(run_id: str, path: str):
    v1 = _with_k8s()

    # Create persistent volume
    pv = client.V1PersistentVolume()
    pv.metadata = client.V1ObjectMeta(name=f'secd-{run_id}-output')
    pv.spec = client.V1PersistentVolumeSpec(
        access_modes=["ReadWriteOnce"],
        capacity={"storage": "50Gi"},
        nfs=client.V1NFSVolumeSource(
            path=path,
            server='172.31.5.11'
        ),
        storage_class_name="nfs",
        persistent_volume_reclaim_policy="Retain",
        volume_mode="Filesystem"
    )

    v1.create_persistent_volume(body=pv)

    # Create persistent volume claim
    pvc = client.V1PersistentVolumeClaim()
    pvc.metadata = client.V1ObjectMeta(name=f'secd-pvc-{run_id}-output')
    pvc.spec = client.V1PersistentVolumeClaimSpec(
        access_modes=["ReadWriteOnce"],
        resources=client.V1ResourceRequirements(
            requests={"storage": "50Gi"}
        ),
        storage_class_name="nfs",
        volume_name=f'secd-{run_id}-output',
        volume_mode="Filesystem"
    )

    v1.create_namespaced_persistent_volume_claim(
        body=pvc, namespace=f"secd-{run_id}")


def delete_by_user_id(user_id: str) -> List[str]:
    v1 = _with_k8s()

    run_ids = []

    # Get all namespaces
    namespaces = v1.list_namespace()
    for namespace in namespaces.items:
        # Get annotations
        annotations = namespace.metadata.annotations
        if annotations is None:
            continue

        if 'userid' not in annotations:
            continue

        # Get userid annotations
        k8s_user_id = annotations.get('userid')

        if user_id == k8s_user_id:
            run_id = namespace.metadata.name.replace("secd-", "")

            log(f"Finishing run {namespace.metadata.name} - new push by userid {user_id} - Delete resources")
            v1.delete_namespace(name=namespace.metadata.name)
            v1.delete_persistent_volume(
                name=f'{namespace.metadata.name}-output')

            run_ids.append(run_id)

    return run_ids


def cleanup_resources() -> List[str]:
    v1 = _with_k8s()

    run_ids = []

    # Get all namespaces
    namespaces = v1.list_namespace()
    for namespace in namespaces.items:
        # Get annotations
        annotations = namespace.metadata.annotations
        if annotations is None:
            continue

        if 'rununtil' not in annotations:
            continue

        # Get rununtil annotation
        rununtil = annotations.get('rununtil')
        if datetime.datetime.fromisoformat(rununtil) < datetime.datetime.now():
            run_id = namespace.metadata.name.replace("secd-", "")

            log(f"Finishing run {namespace.metadata.name} - expired rununtil {rununtil} - Delete resources")
            v1.delete_namespace(name=namespace.metadata.name)

            try:
                v1.delete_persistent_volume(
                    name=f'{namespace.metadata.name}-output')
            except:
                pass


            run_ids.append(run_id)

    return run_ids
