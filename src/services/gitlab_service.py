import os
import gitlab
import yaml
import datetime
import shutil
import subprocess

from typing import Dict
from git import Repo
from cerberus import Validator
from app.src.util.logger import log
from app.src.util.setup import get_settings

class GitlabService:
    def __init__(self):
        self.glSettings = get_settings()['gitlab']
        self.client = gitlab.Gitlab(
            url = self.glSettings['url'],
            private_token = self.glSettings['token']
        )
        try:
            self.client.auth()
        except gitlab.exceptions.GitlabAuthenticationError as e:
            log(f"Authentication failed: {e}", "ERROR")

    def has_file_in_repo(self, project_id: str, file_path: str, ref: str) -> bool:
        client = self.client
        try:
            project = client.projects.get(project_id)
            project.files.get(file_path, ref=ref)
        except gitlab.exceptions.GitlabGetError as e:
            log(f'Details: {e}', "ERROR")
            return False
        return True


    def get_metadata(self, file_path: str) -> Dict[str, str]:
        default = {
            'runfor': 3,
            'gpu': False
        }

        if not os.path.isfile(file_path):
            log(f'No metadata file found at {file_path}. Fallback to default')
            return default

        with open(file_path, 'r') as f:
            metadata = f.read()
            try:
                yaml_metadata = yaml.safe_load(metadata)
            except:
                log(f'Invalid metadata file {file_path}', "ERROR")
                return {}

        if not yaml_metadata:
            log(f'Invalid metadata file {file_path}. Fallback to default')
            return default

        validation = {
            'runfor': {'type': 'number'},
            'gpu': {'type': 'boolean'},
        }

        v = Validator(validation)
        v.allow_unknown = True
        if not v.validate(yaml_metadata):
            log(f'Invalid metadata file {file_path}. Fallback to default')
            return default

        for key in default:
            if key not in yaml_metadata:
                yaml_metadata[key] = default[key]

        return yaml_metadata


    def get_signature(self, project_id: str, commit_id: str) -> Dict[str, any]:
            project = None
            commit = None

            try:
                project = self.client.projects.get(project_id)
                commit = project.commits.get(commit_id)
                gpg_signature = commit.signature()

                return gpg_signature

            except gitlab.exceptions.GitlabGetError as e:
                if project is None:
                    log(f"Project {project_id} not found. Details: {e}", "ERROR")
                elif commit is None:
                    log(f"Commit {commit_id} not found in project {project_id}. Details: {e}", "ERROR")
                else:
                    log(f"No signature found for commit {commit_id} in project {project_id}. Details: {e}", "ERROR")
                raise

            except Exception as e:
                log(f"An unexpected error occurred while processing commit {commit_id} in project {project_id}. Details: {e}", "ERROR")
                raise


    def get_idp_user_id(self, gitlab_user_id: int) -> str:
        client = self.client

        try:
            user = client.users.get(gitlab_user_id)
        except gitlab.exceptions.GitlabGetError as e:
            log(f'User {gitlab_user_id} not found. Details: {e}')
            return None

        if len(user.identities) == 0:
            log(f'User {gitlab_user_id} has no identity providers')
            return None

        if 'extern_uid' not in user.identities[0]:
            log(f'User {gitlab_user_id} has no extern_uid')
            return None

        return user.identities[0]['extern_uid']


    def clone(self, gitlab_url: str, repo_path: str):
        gl_settings = self.glSettings

        gitlab_repo_url = gitlab_url.replace("https://", f"https://{gl_settings['username']}:{gl_settings['password']}@")
        Repo.clone_from(gitlab_repo_url, repo_path)

    def push_results(self, run_id: str):
        repo_path = f"{get_settings()['path']['repoPath']}/{run_id}"

        date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        commit_message = f'secd: Inserting result of run {run_id} finished at {date}'
        branch_name = f'secd-{datetime.datetime.now().strftime("%Y-%m-%d_%H.%M.%S")}-{run_id}'

        if not os.path.exists(repo_path):
            return

        try:
            subprocess.run(["git", "checkout", "-b", branch_name], check=True,
                        cwd=repo_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass

        try:
            subprocess.run(["git", "add", "."], check=True, cwd=repo_path,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass

        try:
            subprocess.run(["git", "commit", "-m", f'"{commit_message}"'], check=True,
                        cwd=repo_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass

        try:
            subprocess.run(["git", "push", "origin", branch_name], check=True,
                        cwd=repo_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass

        shutil.rmtree(repo_path, ignore_errors=True)

    def validate_event_token(self, req):
        event = req.get_header('X-Gitlab-Event')
        if event not in ['Push Hook', 'System Hook']:
            log(f"Invalid X-Gitlab-Event header: {event}", "ERROR")
            raise gitlab.GitlabHeadError(error_message='Invalid X-Gitlab-Event header', response_code=400)
        if req.get_header('X-Gitlab-Token') != get_settings()['gitlab']['secret']:
            log("Unauthorized access: Invalid token", "ERROR")
            raise gitlab.GitlabAuthenticationError(error_message='Unauthorized access: Invalid token', response_code=401)
        return True

    def validate_body_schema(self, body):
        schema = {
            'event_name': {'type': 'string'},
            'ref': {'type': 'string'},
            'user_id': {'type': 'integer'},
            'project_id': {'type': 'integer'},
            'project': {
                'type': 'dict',
                'schema': {
                    'http_url': {'type': 'string'},
                }
            },
            'commits': {
                'type': 'list',
                'schema': {
                    'type': 'dict',
                    'schema': {
                        'id': {'type': 'string'},
                    }
                }
            }
        }
        v = Validator(schema)
        v.allow_unknown = True
        if not v.validate(body):
            log(f"Invalid body: {v.errors}", "ERROR")
            raise gitlab.GitlabError(error_message=f'Invalid body: {v.errors}')

    def validate_body(self, body) -> bool:
        try:

            # Validate user push and not automated push
            if body['ref'].startswith('refs/heads/secd-'):
                log(f"Automated branch push {body['ref']}", "INFO")
                return False
            
            self.validate_body_schema(body)

            # Validate push event
            if body['event_name'] != 'push':
                log(f"Invalid event_name: {body['event_name']}", "ERROR")
                raise gitlab.GitlabError(error_message=f'Invalid event_name: {body["event_name"]}')
            
            # Validate commit from main branch
            if body['ref'] != 'refs/heads/main':
                log(f"Commit is not from main branch: {body['ref']}", "ERROR")
                raise gitlab.GitlabError(error_message=f'Commit is not from main branch: {body["ref"]}')
            
            # Validate commit signature
            for push_commit in body['commits']:
                signature = self.get_signature(body['project_id'], push_commit['id'])

                if signature is None:
                    raise gitlab.GitlabError(f'No signature found for commit {push_commit["id"]}')

                if signature['verification_status'] != 'verified':
                    raise gitlab.GitlabError(f'Signature not verified for commit {push_commit["id"]}')

            # Validate docker file present
            if not self.has_file_in_repo(body['project_id'], 'Dockerfile', body['ref']):
                log(f"No Dockerfile found in project {body['project_id']}", "ERROR")
                raise gitlab.GitlabError(error_message=f'No Dockerfile found in project {body["project_id"]}')
            
            return True

        except gitlab.GitlabError as e:
            raise