import yaml
import os

from cerberus import Validator


def get_settings():
    """Get settings from config file"""
    global settings
    if settings == {}:
        load_settings()
    return settings


def load_settings():
    """Load settings from config file"""

    env_name = "CONFIG_FILE"
    if env_name not in os.environ:
        raise Exception(f'{env_name} is not set')
    else:
        config_path = os.environ[env_name]

    yaml_file = open(config_path, 'r')
    loaded_yaml = yaml.load(yaml_file, Loader=yaml.FullLoader)

    # match against schema
    schema = yaml.load(open('settings-schema.yml', 'r'),
                       Loader=yaml.FullLoader)
    v = Validator(schema)
    if not v.validate(loaded_yaml):
        raise Exception(f'Invalid config file: {v.errors}')

    global settings
    settings = loaded_yaml
