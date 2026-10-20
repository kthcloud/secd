import falcon
import threading
from kubernetes import client, config
from wsgiref.simple_server import make_server

from app.src.util.setup import load_settings, get_settings
from app.src.util.logger import log
from app.src.util.hook import Hook
from app.src.util.daemon import Daemon

from app.src.services.docker_service import DockerService
from app.src.services.gitlab_service import GitlabService
from app.src.services.keycloak_service import KeycloakService
from app.src.services.hook_service import HookService
from app.src.services.vault_service import VaultService
from app.src.services.kubernetes_service import KubernetesService
from app.src.services.kubernetes_services.helm_service import HelmService
from app.src.services.kubernetes_services.namespace_service import NamespaceService
from app.src.services.kubernetes_services.persistent_volume_service import PersistentVolumeService
from app.src.services.kubernetes_services.pod_service import PodService
from app.src.services.kubernetes_services.secret_service import SecretService
from app.src.services.kubernetes_services.service_account_service import ServiceAccountService
from app.src.util.quiet_handler import QuietHandler


class Server:
    def __init__(self):
        log("Starting server....")
        load_settings()

        self.apps = []
        self.threads = []

        # Instantiate core services
        self.init_kubernetes()
        self.keycloak_service = KeycloakService()
        self.docker_service = DockerService()
        self.gitlab_service = GitlabService()
        self.vault_service = VaultService()

        # Instantiate resources services
        self.hook_service = HookService(
            keycloak_service=self.keycloak_service,
            gitlab_service=self.gitlab_service,
            kubernetes_service=self.kubernetes_service,
            docker_service=self.docker_service,
            vault_service=self.vault_service
        )

        self.hook_resource = Hook(hook_service=self.hook_service)

        self.create_app('/v1/hook', self.hook_resource, 8080)

    def create_app(self, path, resource, port):
        app = falcon.App()
        app.add_route(path, resource)
        self.apps.append((app, port))

    def serve_app(self, app, port):
        try:
            with make_server('', port, app, handler_class=QuietHandler) as httpd:
                httpd.serve_forever()
        except Exception as e:
            log(f"Error starting server: {e}", "ERROR")

    def run(self):
        log("Running server...")
        try:
            microk8s_cleanup = Daemon(self.kubernetes_service, self.gitlab_service)
            microk8s_cleanup_thread = threading.Thread(target=microk8s_cleanup.start_microk8s_cleanup)
            microk8s_cleanup_thread.start()

            # Serve each app on different ports in separate threads
            for app, port in self.apps:
                thread = threading.Thread(target=self.serve_app, args=(app, port))
                self.threads.append(thread)
                thread.start()

            # Join threads to keep the main thread running
            for thread in self.threads:
                thread.join()

        except Exception as e:
            log(f"Error starting Daemon thread: {e}", "ERROR")
    
    def init_kubernetes(self):
        self.config_path = get_settings()['k8s']['configPath']
        self.config = client.Configuration()
        config.load_kube_config(config_file=self.config_path, client_configuration=self.config)
        self.config.ssl_ca_cert = '/var/snap/microk8s/current/certs/ca.crt' 

        namespace_service = NamespaceService(config=self.config)    
        pv_service = PersistentVolumeService(config=self.config)
        pod_service = PodService(config=self.config)
        secret_service = SecretService(config=self.config)
        helm_service = HelmService(config=self.config)
        service_account_service = ServiceAccountService(config=self.config)

        self.kubernetes_service = KubernetesService(
            namespace_service=namespace_service,
            pod_service=pod_service,
            pv_service=pv_service,
            secret_service=secret_service,
            helm_service=helm_service,
            service_account_service=service_account_service
        )