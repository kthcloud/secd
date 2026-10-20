import os
import docker
import docker.tls
import docker.errors
from app.src.util.setup import get_settings
from app.src.util.logger import log

class DockerService:
    def __init__(self):
        self.reg_settings = get_settings()['registry']
        self.path_registry_ca = get_settings()['registry']['ca_path']
        try:
            # Configure TLS if a CA certificate path is provided and valid
            if self.path_registry_ca and os.path.exists(self.path_registry_ca):
                tls_config = docker.tls.TLSConfig(
                    ca_cert=self.path_registry_ca,  # Use the self-signed certificate as CA
                    verify=True  # Enforce verification with the provided certificate
                )
                self.client = docker.DockerClient(
                    base_url="unix://var/run/docker.sock",
                    tls=tls_config
                )
            else:
                self.client = docker.from_env()
                log("Docker client initialized without custom TLS configuration", "INFO")
                if not self.path_registry_ca:
                    log("Warning: 'ca_path' not found in registry settings", "WARNING")
                elif not os.path.exists(self.path_registry_ca):
                    log(f"Warning: CA certificate path {self.path_registry_ca} does not exist", "WARNING")
        except docker.errors.DockerException as e:
            log(f"Error initializing Docker client: {str(e)}", "ERROR")
            raise Exception(f"Error initializing Docker client: {e}")

    def build_image(self, repo_path, image_name):
        try:
            self.client.images.build(path=repo_path, tag=image_name)
        except Exception as e:
            log(f"Unexpected error building image {image_name}: {str(e)}", "ERROR")
            raise Exception(f"Unexpected error building image {image_name}: {e}")

    def build_and_push_image(self, repo_path, run_id):
        try:
            image_name = self.generate_image_name(run_id)
            self.build_image(repo_path, image_name)
            self.push_image(image_name)
            return image_name
        except Exception as e:
            log(f"Failed to build and push image {image_name}: {str(e)}", "ERROR")
            raise Exception(f"Failed to build and push image {image_name}: {e}")

    def generate_image_name(self, run_id):
        try:
            # Remove the https:// prefix for the image tag
            return f"{self.reg_settings['url']}/{self.reg_settings['project']}/{run_id}"
        except KeyError as e:
            log(f"Missing registry setting: {str(e)}", "ERROR")
            raise Exception(f"Missing registry setting: {e}")

    def login_to_registry(self):
        try:
            url = self.reg_settings.get("url")
            username = self.reg_settings.get("username")
            password = self.reg_settings.get("password")
            # Use HTTPS since we're trusting the certificate via TLSConfig
            self.client.login(username=username, password=password, registry=f"https://{url}")
        except Exception as e:
            log(f"Unexpected error logging into registry {url}: {str(e)}", "ERROR")
            raise Exception(f"Unexpected error logging into registry {url}: {e}")

    def push_image(self, image_name):
        try:
            self.client.images.push(image_name)
        except Exception as e:
            log(f"Unexpected error pushing image {image_name}: {str(e)}", "ERROR")
            raise Exception(f"Unexpected error pushing image {image_name}: {e}")

    def remove_image(self, image_name):
        try:
            self.client.images.remove(image_name)
        except Exception as e:
            log(f"Unexpected error removing image {image_name}: {str(e)}", "ERROR")
            raise Exception(f"Unexpected error removing image {image_name}: {e}")

    def remove_dangling(self):
        try:
            for image in self.client.images.list():
                if not image.tags:
                    self.client.images.remove(image.id)
        except Exception as e:
            log(f"Unexpected error while removing dangling images: {str(e)}", "ERROR")