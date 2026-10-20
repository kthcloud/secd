import yaml
import os
from cerberus import Validator
from dotenv import load_dotenv
from app.src.util.logger import log

load_dotenv()
settings = {}

def get_settings():
    global settings
    if settings == {}:
        load_settings()
    return settings

def load_settings(config_path=None):
    if config_path is None:
        config_path = os.getenv("CONFIG_FILE")
        if not config_path:
            raise Exception("CONFIG_FILE environment variable is not set and no config_path provided")

    if not os.path.exists(config_path):
        log(f'Config file not found: {config_path}', "ERROR")
        raise Exception(f'Config file not found: {config_path}')

    with open(config_path, 'r') as yaml_file:
        loaded_yaml = yaml.load(yaml_file, Loader=yaml.FullLoader)

    schema_path = "/home/cloud/secd/app/config/settings-schema.yml"
    if not os.path.exists(schema_path):
        log(f'Schema file not found: {schema_path}', "ERROR")
        raise Exception(f'Schema file not found: {schema_path}')

    with open(schema_path, 'r') as schema_file:
        schema = yaml.load(schema_file, Loader=yaml.FullLoader)

    """
    v = Validator(schema) # type: ignore
    if not v.validate(loaded_yaml): # type: ignore
        log(f'Invalid config file: {v.errors}', "ERROR")
        raise Exception(f'Invalid config file: {v.errors}') # type: ignore
    """
    global settings
    settings = loaded_yaml
