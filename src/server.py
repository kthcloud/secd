import falcon
import json
import subprocess
import uuid
import shutil

from git import Repo

import src.gitlab_service as gitlab_service
import src.keycloak_service as keycloak_service
import src.mysql_service as mysql_service
import src.docker_service as docker_service
import src.k8s_service as k8s_service

from wsgiref.simple_server import make_server
from cerberus import Validator

from src.setup import get_settings


class HookResource:
    def on_post(self, req, resp):
        event = req.get_header('X-Gitlab-Event')
        if event != 'Push Hook' and event != 'System Hook':
            raise falcon.HTTPBadRequest(
                title='Bad request',
                description='Invalid X-Gitlab-Event header'
            )

        if req.get_header('X-Gitlab-Token') != get_settings()['gitlab']['secret']:
            raise falcon.HTTPUnauthorized(
                title='Unauthorized',
                description='Invalid token'
            )

        # parse body
        body_raw = req.bounded_stream.read()
        if not body_raw:
            raise falcon.HTTPBadRequest(
                title='Bad request',
                description='Missing body'
            )

        try:
            body = json.loads(body_raw)
        except:
            raise falcon.HTTPBadRequest(
                title='Bad request',
                description='Invalid body'
            )

        validation = {
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

        v = Validator(validation)
        v.allow_unknown = True
        if not v.validate(body):
            raise falcon.HTTPBadRequest(
                title='Bad request',
                description=f'Invalid body: {v.errors}'
            )

        print(f'found {len(body["commits"])} commits')
        for push_commit in body['commits']:

            signature = gitlab_service.get_signature(
                body['project_id'], push_commit['id'])
            if signature is None:
                raise falcon.HTTPBadRequest(
                    title='Bad request',
                    description=f'No signature found for commit {push_commit["id"]}'
                )

            if signature['verification_status'] != 'verified':
                print(
                    f'Found signature, but it is not verified: {signature["verification_status"]}')

        print(f'All {len(body["commits"])} commits have a verified signature')

        # check if the project has a Dockerfile
        if not gitlab_service.has_file_in_repo(body['project_id'], 'Dockerfile', body['ref']):
            raise falcon.HTTPBadRequest(
                title='Bad request',
                description=f'No Dockerfile found in project {body["project_id"]}'
            )

        # Commit is ok, return 200
        resp.status = falcon.HTTP_200

        # Find keycloak user id
        gitlab_user_id = body['user_id']
        keycloak_user_id = gitlab_service.get_idp_user_id(gitlab_user_id)

        # fetch keycloak user groups
        keycloak_groups = keycloak_service.get_keycloak_user_groups(
            keycloak_user_id)

        # create db user with groups
        mysql_groups = []
        for group in keycloak_groups:
            prefix = '/mysql_'
            if group['path'].startswith(prefix):
                mysql_groups.append(group['path'][len(prefix):])

        db_user, db_pass = mysql_service.create_mysql_user(mysql_groups)

        # Run ID
        run_id = uuid.uuid4()

        # Clone repo
        repo_path = f"/home/emil/kthcloud-secure/{run_id}"
        gitlab_repo_url = body["project"]["http_url"].replace("https://", "https://app:xSDrHkmhDyorLjbzrFu2HIxrXqkmdc71dY6M7oo@")
        Repo.clone_from(gitlab_repo_url, repo_path)

        # Build image
        reg_settings = get_settings()['registry']
        image_name = f"{reg_settings['url']}/{reg_settings['project']}/{run_id}"
        docker_service.build_image(repo_path, image_name)
        # image_name = "registry.cloud.cbh.kth.se/secure/92dfc8ff-0cca-4592-a186-1dc4f93a95a9"

        docker_service.push_and_remove_image(image_name)

        # Delete repo
        shutil.rmtree(repo_path)

        # Run pod
        k8s_service.create_namespace(run_id)
        k8s_service.create_persistent_volume(keycloak_user_id, run_id)
        k8s_service.create_pod(keycloak_user_id, run_id, image_name, {
                               "DB_USER": db_user, "DB_PASS": db_pass})


app = falcon.App()
app.add_route('/v1/hook', HookResource())


def run():
    with make_server('', 8080, app) as httpd:
        print('Serving on port 8080...')

        # Serve until process is killed
        httpd.serve_forever()
