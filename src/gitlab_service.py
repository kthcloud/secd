import gitlab

from typing import Dict
from src.setup import get_settings


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
        print(f'Project {project_id} not found. Details: {e}')
        return None

    try:
        project.files.get(file_path, ref=ref)
    except gitlab.exceptions.GitlabGetError as e:
        print(f'File {file_path} not found. Details: {e}')
        return False

    return True


def get_signature(project_id: str, commit_id: str) -> Dict[str, any]:
    client = _with_gitlab_client()

    try:
        project = client.projects.get(project_id)
    except gitlab.exceptions.GitlabGetError as e:
        print(f'Project {project_id} not found. Details: {e}')
        return None

    try:
        commit = project.commits.get(commit_id)
    except gitlab.exceptions.GitlabGetError as e:
        print(f'Commit {commit_id} not found. Details: {e}')

    try:
        gpg_signature = commit.signature()
    except gitlab.exceptions.GitlabGetError as e:
        print(
            f'No signature found for {commit_id} (project {project_id}). Details: {e}')
        gpg_signature = None

    return gpg_signature

def get_idp_user_id(gitlab_user_id: int) -> str:
    client = _with_gitlab_client()

    try:
        user = client.users.get(gitlab_user_id)
    except gitlab.exceptions.GitlabGetError as e:
        print(f'User {gitlab_user_id} not found. Details: {e}')
        return None

    if len(user.identities) == 0:
        print(f'User {gitlab_user_id} has no identity providers')
        return None

    if 'extern_uid' not in user.identities[0]:
        print(f'User {gitlab_user_id} has no extern_uid')
        return None

    return user.identities[0]['extern_uid']