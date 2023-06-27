import falcon
import json

import src.service as service

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
            'project_id': {'type': 'integer'},
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

            signature = service.get_signature(
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

        resp.status = falcon.HTTP_200


app = falcon.App()
app.add_route('/v1/hook', HookResource())


def run():
    with make_server('', 8080, app) as httpd:
        print('Serving on port 8080...')

        # Serve until process is killed
        httpd.serve_forever()
