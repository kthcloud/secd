import docker
from src.setup import get_settings


def _with_docker():
    client = docker.from_env()
    return client

def build_image(repo_path, image_name):
    client = _with_docker()
    build_res = client.images.build(path=repo_path, tag=image_name)
    print(build_res)


def push_and_remove_image(image_name):
    client = _with_docker()
    regSettings = get_settings()["registry"]

    url = regSettings.get("url")
    username = regSettings.get("username")
    password = regSettings.get("password")

    
    login_res = client.login(username=username, password=password, registry=url, dockercfg_path="/home/emil/kthcloud-secure/config/config.json") 
    print(login_res)
    
    push_res = client.images.push(image_name)
    print(push_res)

    # client.images.remove(image_name)

    # remove_dangling()

def remove_dangling():
    client=_with_docker()

    for image in client.images.list():
        if image.tags == None:
            client.images.remove(image.id)
