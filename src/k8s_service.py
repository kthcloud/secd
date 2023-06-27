from kubernetes import client, config
from typing import Dict


def _with_k8s():
    config.load_kube_config()
    v1 = client.CoreV1Api()
    return v1


def create_namespace(run_id: str):
    v1 = _with_k8s()

    # create namespace if not exists
    namespace = client.V1Namespace()
    namespace.metadata = client.V1ObjectMeta(name=run_id)
    v1.create_namespace(body=namespace)


def create_pod(run_id: str, image: str, envs: Dict[str, str]):
    v1 = _with_k8s()

    k8s_envs = []
    for env in envs:
        k8s_envs.append(client.V1EnvVar(name=env, value=envs[env]))

    # Create pod
    pod = client.V1Pod()
    pod.metadata = client.V1ObjectMeta(name=f"{image}_{run_id}")
    pod.spec = client.V1PodSpec(
        volumes=[
            client.V1Volume(
                name=f'vol_{run_id}_output',
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=f'pvc_{run_id}_output'
                )
            )
        ],
        containers=[
            client.V1Container(
                name=f"{image}_{run_id}",
                image=image,
                env=k8s_envs,
                volume_mounts=[
                    client.V1VolumeMount(
                        name=f'vol_{run_id}_output',
                        mount_path='/outputs'
                    )
                ],
                lifecycle=client.V1Lifecycle(
                    post_start=client.V1Handler(
                        exec=client.V1ExecAction(
                            command=['mkdir', '-p', f'/outputs/{run_id}']
                        )
                    )
                )
            )
        ]
    )
    v1.create_namespaced_pod(namespace=run_id, body=pod)


def create_persistent_volume(username: str, run_id: str):
    v1 = _with_k8s()

    # Create persistent volume
    pv = client.V1PersistentVolume()
    pv.metadata = client.V1ObjectMeta(name=f'pv_{run_id}_output')
    pv.spec = client.V1PersistentVolumeSpec(
        access_modes=["ReadWriteOnce"],
        capacity={"storage": "50Gi"},
        nfs=client.V1NFSVolumeSource(
            path=f'/mnt/cloud/apps/sec/secure/outputs',
            server='172.31.5.11'
        ),
        storage_class_name="nfs",
        persistent_volume_reclaim_policy="Retain",
        volume_mode="Filesystem"
    )

    v1.create_persistent_volume(body=pv)

    # Create persistent volume claim
    pvc = client.V1PersistentVolumeClaim()
    pvc.metadata = client.V1ObjectMeta(name=f'pvc_{run_id}_output')
    pvc.spec = client.V1PersistentVolumeClaimSpec(
        access_modes=["ReadWriteOnce"],
        resources=client.V1ResourceRequirements(
            requests={"storage": "50Gi"}
        ),
        storage_class_name="nfs",
        volume_name=f'pv_{run_id}_output',
        volume_mode="Filesystem"
    )

    v1.create_namespaced_persistent_volume_claim(body=pvc, namespace=username)
