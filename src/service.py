import gitlab

from typing import Dict
from src.setup import get_settings

def get_signature(project_id: str, commit_id: str) -> Dict[str, any]:
    glSettings = get_settings()['gitlab']


    client = gitlab.Gitlab(glSettings['url'], private_token=glSettings['token'])

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
        print(f'No signature found for {commit_id} (project {project_id}). Details: {e}')
        gpg_signature = None

    return gpg_signature