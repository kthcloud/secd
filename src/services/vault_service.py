import hvac

from app.src.util.setup import get_settings
from app.src.util.logger import log


class VaultService():
    def __init__(self):
        vault_addr = get_settings()['vault']['address']
        vault_token = get_settings()['vault']['token']
        self.client = hvac.Client(url=vault_addr, token=vault_token)
        if not self.client.is_authenticated():
            raise Exception("Failed to authenticate with Vault")
        self.enable_database_secrets_engine()
        self.enable_kubernetes_auth_method()

    def enable_database_secrets_engine(self, path: str = "database") -> None:
        try:
            self.client.sys.enable_secrets_engine(
                backend_type="database",
                path=path
            )
        except Exception as e:
            if "path is already in use" in str(e):
                print(f"Database secrets engine already enabled at {path}")
            else:
                raise Exception(f"Failed to enable database secrets engine: {str(e)}")

    def enable_kubernetes_auth_method(self, path: str = "kubernetes") -> None:
        try:
            self.client.sys.enable_auth_method(
                method_type="kubernetes",
                path=path
            )
        except Exception as e:
            if "path is already in use" in str(e):
                print(f"Kubernetes auth method already enabled at {path}")
            else:
                raise Exception(f"Failed to enable Kubernetes auth method: {str(e)}")
    

    def configure_database_connection(
        self,
        database_name: str,
        db_type: str,
        connection_url_template: str,
        admin_username: str,
        admin_password: str,
        allowed_roles: list,
        path: str = "database"
    ) -> None:
        try:
            self.client.secrets.database.configure(
                name=database_name,
                plugin_name=f"{db_type}-database-plugin",
                mount_point=path,
                connection_url=connection_url_template,
                allowed_roles=allowed_roles,
                username=admin_username,
                password=admin_password,
            )
        except Exception as e:
            raise Exception(f"Failed to configure database connection for {database_name}: {str(e)}")

    def create_database_role(
        self,
        role_name: str,
        database_name: str,
        creation_statements: list,
        default_ttl: str = "1h",
        max_ttl: str = "24h",
        path: str = "database"
    ) -> None:
        try:
            self.client.secrets.database.create_role(
                name=role_name,
                db_name=database_name,
                creation_statements=creation_statements,
                default_ttl=default_ttl,
                max_ttl=max_ttl,
                mount_point=path
            )
        except Exception as e:
            raise Exception(f"Failed to create database role {role_name}: {str(e)}")

    
    def create_kubernetes_auth_role(
        self,
        role_name: str,
        service_account_name: str,
        service_account_namespace: str,
        policy: str,
        ttl: str = "1h"
    ) -> None:
        try:
            self.client.auth.kubernetes.create_role(
                name=role_name,
                bound_service_account_names=[service_account_name],
                bound_service_account_namespaces=[service_account_namespace],
                policies=[policy],
                ttl=ttl
            )
        except Exception as e:
            raise Exception(f"Failed to create Kubernetes auth role {role_name}: {str(e)}")

    def create_policy(self, policy_name: str, policy_rules: str) -> None:
        try:
            self.client.sys.create_or_update_policy(
                name=policy_name,
                policy=policy_rules
            )
        except Exception as e:
            raise Exception(f"Failed to create policy {policy_name}: {str(e)}")

    def delete_kubernetes_auth_role(self, role_name: str) -> None:
        try:
            self.client.auth.kubernetes.delete_role(name=role_name)
        except Exception as e:
            raise Exception(f"Failed to delete Kubernetes auth role {role_name}: {str(e)}")
