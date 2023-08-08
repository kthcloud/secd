import os
import gitlab
import yaml
import datetime
import shutil
import subprocess

from typing import Dict
from git import Repo
from cerberus import Validator
from src.setup import get_settings
from src.logger import log

def _with_gitlab_client() -> gitlab.Gitlab:
    glSettings = get_settings()['gitlab']
    client = gitlab.Gitlab(
        glSettings['url'], private_token=glSettings['token'])

    return client


def has_file_in_repo(project_id: str, file_path: str, ref: str) -> bool:
    client = _with_gitlab_client()

    try:
        project = client.projects.get(project_id)
    except gitlab.exceptions.GitlabGetError as e:
        log(f'Project {project_id} not found. Details: {e}', "ERROR")
        return None

    try:
        project.files.get(file_path, ref=ref)
    except gitlab.exceptions.GitlabGetError as e:
        log(f'File {file_path} not found. Details: {e}', "ERROR")
        return False

    return True


def get_metadata(file_path: str) -> Dict[str, any]:
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
            return None

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


def get_signature(project_id: str, commit_id: str) -> Dict[str, any]:
    client = _with_gitlab_client()

    try:
        project = client.projects.get(project_id)
    except gitlab.exceptions.GitlabGetError as e:
        log(f'Project {project_id} not found. Details: {e}', "ERROR")
        return None

    try:
        commit = project.commits.get(commit_id)
    except gitlab.exceptions.GitlabGetError as e:
        log(f'Commit {commit_id} not found. Details: {e}', "ERROR")
        return None

    try:
        gpg_signature = commit.signature()
    except gitlab.exceptions.GitlabGetError as e:
        log(
            f'No signature found for {commit_id} (project {project_id}). Details: {e}', "ERROR")
        gpg_signature = None

    return gpg_signature


def get_idp_user_id(gitlab_user_id: int) -> str:
    client = _with_gitlab_client()

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


def clone(gitlab_url: str, repo_path: str):
    gl_settings = get_settings()['gitlab']

    # add credentials to url
    gitlab_repo_url = gitlab_url.replace(
        "https://", f"https://{gl_settings['username']}:{gl_settings['password']}@")
    Repo.clone_from(gitlab_repo_url, repo_path)

def push_results(run_id: str):
    repo_path = f"{get_settings()['repoPath']}/{run_id}"

    date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    commit_message = f'secd: Inserting result of run {run_id} finished at {date}'
    branch_name = f'output-{date.replace(":", ".").replace(" ", "_")}-{run_id}'
    # check if repo_path exists
    if not os.path.exists(repo_path):
        return

    try:
        subprocess.run(["git", "checkout", "-b", f"outputs-{branch_name}"], check=True, cwd=repo_path, stdout = subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass
    
    try:
        subprocess.run(["git", "add", "."], check=True, cwd=repo_path, stdout = subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass
    
    try:
        subprocess.run(["git", "commit", "-m", f'"{commit_message}"'], check=True, cwd=repo_path, stdout = subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass
    
    try:
        subprocess.run(["git", "push", "origin", f"outputs-{run_id}"], check=True, cwd=repo_path, stdout = subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass
    
    shutil.rmtree(repo_path, ignore_errors=True)
