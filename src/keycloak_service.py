import keycloak

from typing import Dict, List
from src.setup import get_settings


def _with_keycloak_client() -> keycloak.KeycloakAdmin:
    kcSettings = get_settings()['keycloak']
    client = keycloak.KeycloakAdmin(
        server_url=kcSettings['url'],
        username=kcSettings['username'],
        password=kcSettings['password'],
        realm_name=kcSettings['realm'],
        verify=True
    )

    return client


def get_keycloak_user_groups(keycloak_user_id: str) -> List[str]:
    client = _with_keycloak_client()

    try:
        groups = client.get_user_groups(keycloak_user_id)
    except keycloak.exceptions.KeycloakGetError as e:
        print(f'User {keycloak_user_id} not found. Details: {e}')
        return None

    return groups
