from keycloak import KeycloakAuthenticationError, KeycloakGetError, KeycloakAdmin, KeycloakOpenIDConnection, KeycloakPostError, KeycloakOpenID
from typing import Dict, List
from app.src.util.setup import get_settings
from app.src.util.logger import log

class KeycloakService:
    def __init__(self):
        self.kc_settings = get_settings()['keycloak']
        keycloak_connection = KeycloakOpenIDConnection(
            server_url=self.kc_settings['url'],
            username=self.kc_settings['username'],
            password=self.kc_settings['password'],
            realm_name=self.kc_settings['realm'],
            client_id=self.kc_settings['admin-cli']['client_id'],
        )
        self.keycloak_admin = KeycloakAdmin(connection=keycloak_connection)

        self.keycloak_openid = KeycloakOpenID(
            server_url=self.kc_settings['url'],
            realm_name=self.kc_settings['realm'],
            client_id=self.kc_settings['database-service']['client_id'],
            client_secret_key=self.kc_settings['database-service']['client_secret'],
        )

    def create_temp_user(self, username: str, password: str) -> str:
        client = self.keycloak_admin
        UserRepresentation = {
            "username": username,
            "enabled": True,
            "credentials": [{"type": "password", "value": password, "temporary": True}],
            "firstName": "Temp",
            "lastName": "User",
            "email": f"{username}@example.com",
            "emailVerified": False
        }

        try:
            user_id = client.create_user(UserRepresentation)
        except KeycloakGetError as e:
            log(f'Error creating user {username}. Details: {e}', "ERROR")

        return user_id

    def delete_temp_user(self, user_id: str) -> bool:
        client = self.keycloak_admin

        try:
            client.delete_user(user_id)
        except KeycloakGetError as e:
            log(f'Error deleting user {user_id}. Details: {e}', "ERROR")
            return False

        return True

    def assign_role_to_user(self, user_id: str, client_id: str, role: str) -> bool:
        client = self.keycloak_admin

        try:
            # Fetch clients
            clients = client.get_clients()
            internal_client_id = next((c['id'] for c in clients if c['clientId'] == client_id), None)
            if not internal_client_id:
                log(f'Error: Client {client_id} not found', "ERROR")
                return False

            # Fetch roles
            roles = client.get_client_roles(client_id=internal_client_id)
            role_representation = next((r for r in roles if r['name'] == role), None)
            if role_representation is None:
                log(f'Error: Role {role} not found for client {client_id}', "ERROR")
                return False

            # Assign role to user
            client.assign_client_role(user_id=user_id, client_id=internal_client_id, roles=[role_representation])

        except KeycloakGetError as e:
            log(f'Error fetching clients or roles: {e.response_code}, Details: {e.response_body}', "ERROR")
            return False
        except KeycloakPostError as e:
            log(f'Error assigning role {role} to user {user_id}. Details: {e}', "ERROR")
            return False
        except Exception as e:
            log(f'Unexpected error: {str(e)}', "ERROR")
            return False

        return True

    def validate(self, auth_header):
            try:
                parts = auth_header.split(' ')
                if len(parts) != 2 or parts[0].lower() != 'bearer':
                    raise KeycloakAuthenticationError("Invalid authorization header format")

                token = parts[1]

                if not token:
                    raise KeycloakAuthenticationError("Authorization token required")

                userinfo = self.keycloak_openid.introspect(token)

                if userinfo.get('active'):
                    return True
                else:
                    return False

            except KeycloakAuthenticationError as e:
                log(f"Token validation error: {str(e)}", "ERROR")
                raise KeycloakAuthenticationError("Invalid authorization token")
            except Exception as e:
                log(f"Unexpected error during token validation: {str(e)}", "ERROR")
                raise KeycloakAuthenticationError("Error during token validation")

    def get_access_token_username_password(self, username, password) -> dict[str, str]:
        try:
            token = self.keycloak_openid.token(username=username, password=password, grant_type="client_credentials")
            return token

        except Exception as e:
            log(f"Error obtaining access token: {str(e)}", "ERROR")
            return {}

    def get_user_realm_roles(self,user_id: str) -> Dict[str, any]:
        client = self.keycloak_admin
        try:
            roles = client.get_realm_roles_of_user(user_id)
        except KeycloakGetError as e:
            log(f'Error fetching realm roles for user {user_id}. Details: {e}', "ERROR")
            return None
        return roles

    def get_user_groups(self,user_id: str) -> List[Dict[str, any]]:
        client = self.keycloak_admin
        try:
            groups = client.get_user_groups(user_id=user_id)
        except KeycloakGetError as e:
            log(f'Error fetching groups for user {user_id}. Details: {e}', "ERROR")
            return []
        return groups

    def get_user_client_roles(self,user_id: str, client_id: str) -> List[Dict[str, any]]:
        client = self.keycloak_admin
        try:
            clients = client.get_clients()
            client_internal_id = next((c['id'] for c in clients if c['clientId'] == client_id), None)

            if not client_internal_id:
                log(f'Client with clientId {client_id} not found', "ERROR")
                return []

            roles = client.get_client_roles_of_user(user_id=user_id, client_id=client_internal_id)
        except KeycloakGetError as e:
            log(f'Error fetching client roles for user {user_id} in client {client_id}. Details: {e}', "ERROR")
            return []
        return roles

    def check_user_in_group(self,user_id: str, group_name: str) -> bool:
        groups = self.get_user_groups(user_id)
        group_names = [group['name'] for group in groups]
        if group_name in group_names:
            return True
        else:
            log(f"User is not part of the '{group_name}' group.")
            return False

    def check_user_has_role(self,user_id: str, client_id: str, role_name: str) -> bool:
        roles = self.get_user_client_roles(user_id, client_id)
        role_names = [role['name'] for role in roles]
        if role_name in role_names:
            return True
        else:
            log(f"User does not have the '{role_name}' role.")
            return False

