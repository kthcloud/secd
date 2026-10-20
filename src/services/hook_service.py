import datetime
import os
import uuid
from typing import Dict, Any
from app.src.util.logger import log
from app.src.util.setup import get_settings
from app.src.services.kubernetes_service import KubernetesService
from app.src.services.vault_service import VaultService
from app.src.services.gitlab_service import GitlabService
from app.src.services.keycloak_service import KeycloakService
from app.src.services.docker_service import DockerService
from app.src.dto.run import Run

SECD_GROUP = "secd"
STORAGE_TYPE = "storage"
DATABASE_SERVICE = "database-service"
STORAGE_SIZE = "100Gi"
OUTPUT_STORAGE_SIZE = "50Gi"

class HookService():
    def __init__(
        self,
        gitlab_service: GitlabService,
        keycloak_service: KeycloakService,
        docker_service: DockerService,
        kubernetes_service: KubernetesService,
        vault_service: VaultService,
    ):
        self.gitlab_service = gitlab_service
        self.keycloak_service = keycloak_service
        self.docker_service = docker_service
        self.kubernetes_service = kubernetes_service
        self.vault_service = vault_service

    def create(self, body: Dict[str, Any]):
        try:
            run = self._init(body)
            if run == None: # Automated push
                return

            if run.database_type == "file": 
                self._database_is_file(run)

            elif run.database_type == "mysql": 
                self._database_is_mysql(run)

            else:
                log(f"database_type not implemented: {run.database_type}", "WARNING")

        except Exception as e:
            log(f"Error in create process: {str(e)}", "ERROR")

    def _database_is_mysql(self, run:Run):
        self._create_namespace(run)
        self._create_pv(run)
        self._setup_pvc(run)
        self._vault_setup(run)
        self._create_pod_by_vault(run)

    def _database_is_file(self, run:Run):
        self._create_namespace(run)
        self._create_pv(run)
        self.kubernetes_service.pod_service.create_nfs_pod(
            database_name=run.database_name,
            run_id=run.run_id,
            image_name=run.image_name,
            environment_variables=run.env_vars
        )

    def _init(self, body: Dict[str, Any]) -> Run:
        try: 
            # 1) Validate & init run
            if not self.gitlab_service.validate_body(body):
                return None
            
            run = Run()

            gitlab_user_id = body['user_id']
            run.keycloak_user_id = self.gitlab_service.get_idp_user_id(int(gitlab_user_id))
            if not self.keycloak_service.check_user_in_group(run.keycloak_user_id, SECD_GROUP):
                raise Exception(f"User {run.keycloak_user_id} not in '{SECD_GROUP}'")

            self.gitlab_service.clone(body["project"]["http_url"], run.repo_path)
            os.makedirs(run.output_path, exist_ok=True)

            # 2) Metadata & permissions
            run.metadata       = self.gitlab_service.get_metadata(f"{run.repo_path}/secd.yml")
            run.database_name  = run.metadata['database_name']
            if not self.keycloak_service.check_user_has_role(
                run.keycloak_user_id, DATABASE_SERVICE, run.database_name):
                raise Exception("User does not have the required DB role")

            # 3) Build the container image
            self.docker_service.login_to_registry()
            run.image_name = self.docker_service.build_and_push_image(run.repo_path, run.run_id)

            # 4) Fill remaining DTO fields
            run.database_type    = run.metadata["database_type"]
            run.run_for          = run.metadata["runfor"]
            run.namespace_labels = {"name": run.database_name}
            run.service_name     = f"service-{run.database_name}.storage.svc.cluster.local"
            run.env_vars = {
                "DB_HOST":     run.service_name,
                "NFS_PATH":    "/data",
                "OUTPUT_PATH": "/output",
                "SECD":        "PRODUCTION",
                "DB_USER":     "",
                "DB_PASS":     "",
            }

            return run
        
        except Exception as e:
            log(f"Failed to initialise run: {str(e)}", "ERROR")



    def _create_pv(self, run:Run):
        self.kubernetes_service.pv_service.create_persistent_volume(
            name = run.pv_name_output, 
            path = f"{run.pvc_repo_path}/repos/{run.run_id}/outputs/{run.date}-{run.run_id}"
        )

    def _create_namespace(self, run:Run):
        self.kubernetes_service.create_namespace(
            user_id = run.keycloak_user_id, 
            run_id = run.run_id, 
            run_for = run.run_for,
            labels = run.namespace_labels,
        )

    def _setup_pvc(self, run:Run) -> str:
        # Fetch the database pod using the correct label selector
        db_pod = self.kubernetes_service.pod_service.get_pod_by_label(
            label_selector=f"name={run.database_name}",  # e.g., "name=mysql-1"
            namespace=STORAGE_TYPE
        )
        if not db_pod:
            log(f"No pod found for database {run.database_name} in namespace {STORAGE_TYPE}", "ERROR")
            raise Exception(f"Database pod not found for {run.database_name}")

        # Extract PVC name from database pod's volume spec
        run.pvc_name = None
        for volume in db_pod.spec.volumes:
            if volume.persistent_volume_claim:
                run.pvc_name = volume.persistent_volume_claim.claim_name  # e.g., "pvc-storage-mysql-1"
                break
        if not run.pvc_name:
            log(f"No PVC found in pod for database {run.database_name}", "ERROR")
            raise Exception("No PVC associated with the database pobd")

        # Fetch the PV bound to this PVC
        pvc = self.kubernetes_service.pv_service.get_pvc(run.pvc_name, namespace=STORAGE_TYPE)
        if not pvc or not pvc.spec.volume_name:
            log(f"No PV bound to PVC {run.pvc_name} in namespace {STORAGE_TYPE}", "ERROR")
            raise Exception(f"PV not found for PVC {run.pvc_name}")

        # Proceed with existing PVC setup for output
        self.kubernetes_service.pv_service.create_persistent_volume_claim(
            run.pvc_name_output, 
            run.namespace, 
            run.pv_name_output, 
            storage_size=OUTPUT_STORAGE_SIZE, 
            access_modes=["ReadWriteOnce"]
        )

        if run.database_type == "file": 
            return run.pvc_name
        else:
            run.pvc_name = None
            return ""


    def _create_pod_by_vault(self, run:Run):

        cache_dir, mount_path = self.kubernetes_service.handle_cache_dir(run.metadata, run.keycloak_user_id, run.run_id)
        db_pod = self.kubernetes_service.pod_service.get_pod_by_label(
            label_selector=f"name={run.metadata['database_name']}",
            namespace=STORAGE_TYPE 
        )

        db_label = db_pod.metadata.labels.get('name')
        self.kubernetes_service.pod_service.create_pod_by_vault(
                run_id = run.run_id,
                image = run.image_name,
                envs = run.env_vars,
                gpu = run.metadata['gpu'],
                mount_path = mount_path,
                database = db_label,
                namespace = run.namespace,
                pvc_name = run.pvc_name,
                vault_role = run.vault_role_name  # Match Vault role from _vault_setup
            )

    def _vault_setup(self, run:Run) -> str:
        # Define database details
        #self.database_name = "mysql-1"
        #self.database_type = "mysql"

        # Define resource names
        run.vault_role_name = f"role-{run.database_name}"  # e.g., role-mysql-1 (for database creds)
        policy_name = f"policy-{run.database_name}"  # e.g., policy-mysql-1
        service_account_name = f"sa-{run.database_name}"  # e.g., sa-mysql-1 (in pod's self.namespace)
        k8s_auth_role_name = f"role-{run.database_name}-{run.namespace}"  # e.g., role-mysql-1-secd-<self.run_id>

        # Step 1: Configure Vault database connection
        connection_url_template = f"{{{{username}}}}:{{{{password}}}}@tcp(service-{run.database_name}.storage.svc.cluster.local:3306)/"
        allowed_roles = [run.vault_role_name]
        username = "vault"
        password = "vaultpassword"
        self.vault_service.configure_database_connection(
            database_name=run.database_name,
            db_type=run.database_type,
            connection_url_template=connection_url_template,
            allowed_roles=allowed_roles,
            admin_username=username,
            admin_password=password
        )

        # Step 2: Create database role for temporary users
        creation_statements = [
            "CREATE USER '{{name}}'@'%' IDENTIFIED BY '{{password}}';",
            "GRANT SELECT ON *.* TO '{{name}}'@'%';"
        ]
        self.vault_service.create_database_role(
            role_name=run.vault_role_name,
            database_name=run.database_name,
            creation_statements=creation_statements,
        )

        # Step 3: Create policy for accessing temporary credentials
        policy_rules = f"""
            path "database/creds/{run.vault_role_name}" {{
                capabilities = ["read"]
            }}
        """
        self.vault_service.create_policy(
            policy_name=policy_name,
            policy_rules=policy_rules
        )

        # Step 4: Create service account in the pod's self.namespace
        self.kubernetes_service.create_service_account(service_account_name, run.namespace)

        # Step 5: Create Kubernetes auth role for the pod's self.namespace
        self.vault_service.create_kubernetes_auth_role(
            role_name=k8s_auth_role_name,
            service_account_name=service_account_name,
            service_account_namespace=run.namespace,  # Pod's self.namespace (e.g., secd-<self.run_id>)
            policy=policy_name
        )
        return run.vault_role_name