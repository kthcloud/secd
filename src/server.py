import falcon
import json
import uuid
import shutil
import threading
import os
import datetime

import src.gitlab_service as gitlab_service
import src.keycloak_service as keycloak_service
import src.mysql_service as mysql_service
import src.docker_service as docker_service
import src.k8s_service as k8s_service
import src.daemon as daemon

from wsgiref.simple_server import make_server
from cerberus import Validator

from src.setup import get_settings
from src.logger import log


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

        v = Validator(validation)
        v.allow_unknown = True
        if not v.validate(body):
            raise falcon.HTTPBadRequest(
                title='Bad request',
                description=f'Invalid body: {v.errors}'
            )

        if body['event_name'] != 'push':
            raise falcon.HTTPBadRequest(
                title='Bad request',
                description=f'Invalid event_name: {body["event_name"]}'
            )

        # check if commit is from main branch
        if body['ref'] != 'refs/heads/main':
            log(f'Commit is not from main branch: {body["ref"]}')
            raise falcon.HTTPBadRequest(
                title='Bad request',
                description=f'Commit is not from main branch: {body["ref"]}'
            )

        log(f'found {len(body["commits"])} commits - {body["project"]["path_with_namespace"]}')
        for push_commit in body['commits']:

            signature = gitlab_service.get_signature(
                body['project_id'], push_commit['id'])
            if signature is None:
                raise falcon.HTTPBadRequest(
                    title='Bad request',
                    description=f'No signature found for commit {push_commit["id"]}'
                )

            if signature['verification_status'] != 'verified':
                log(
                    f'Found signature, but it is not verified: {signature["verification_status"]}')

        log(f'All {len(body["commits"])} commits have a verified signature')

        # check if the project has a Dockerfile
        if not gitlab_service.has_file_in_repo(body['project_id'], 'Dockerfile', body['ref']):
            raise falcon.HTTPBadRequest(
                title='Bad request',
                description=f'No Dockerfile found in project {body["project_id"]}'
            )

        def create():
            try:
                # Find keycloak user id
                gitlab_user_id = body['user_id']
                keycloak_user_id = gitlab_service.get_idp_user_id(
                    gitlab_user_id)

                # fetch keycloak user groups
                keycloak_groups = keycloak_service.get_keycloak_user_groups(
                    keycloak_user_id)

                # create db user with groups
                mysql_groups = []
                for group in keycloak_groups:
                    prefix = '/mysql_'
                    if group['path'].startswith(prefix):
                        mysql_groups.append(group['path'][len(prefix):])

                db_user, db_pass = mysql_service.create_mysql_user(
                    mysql_groups)

                # Run ID
                run_id = str(uuid.uuid4()).replace('-', '')

                # Clone repo
                repo_path = f"{get_settings()['repoPath']}/{run_id}"
                gitlab_service.clone(body["project"]["http_url"], repo_path)

                # Create an output folder for the run
                date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                output_path = f'{repo_path}/outputs/{date}-{run_id}'
                os.makedirs(output_path)

                # Get runfor
                run_meta = gitlab_service.get_metadata(f"{repo_path}/secd.yml")
                run_for = run_meta['runfor']
                gpu = run_meta['gpu']

                # Build image
                reg_settings = get_settings()['registry']
                image_name = f"{reg_settings['url']}/{reg_settings['project']}/{run_id}"
                docker_service.build_image(repo_path, image_name)
                docker_service.push_and_remove_image(image_name)

                # Cleanup user's existing pods
                # deleted_run_ids = k8s_service.delete_by_user_id(
                #     keycloak_user_id)
                # for deleted_run_id in deleted_run_ids:
                #     gitlab_service.push_results(deleted_run_id)

                # Run pod
                k8s_service.create_namespace(keycloak_user_id, run_id, run_for)
                k8s_service.create_persistent_volume(
                    run_id, f'/mnt/cloud/apps/sec/secure/repos/{run_id}/outputs/{date}-{run_id}')
                k8s_service.create_pod(run_id, image_name, {
                    "DB_USER": db_user,
                    "DB_PASS": db_pass,
                    "DB_HOST": 'mysql.mysql.svc.cluster.local',
                    "OUTPUT_PATH": '/output',
                }, gpu)
            except Exception as e:
                log(str(e), "ERROR")
            else:
                log(f"Successfully launched {run_id}")

        threading.Thread(target=create).start()

        # Commit is ok, return 200
        resp.status = falcon.HTTP_200


app = falcon.App()
app.add_route('/v1/hook', HookResource())


def run():
    # Run daemon.run() in a new thread
    daemon_thread = threading.Thread(target=daemon.run)
    daemon_thread.start()

    with make_server('', 8080, app) as httpd:
        log('Serving on port 8080...')

        # Serve until process is killed
        httpd.serve_forever()
