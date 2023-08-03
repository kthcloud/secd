import docker
from src.setup import get_settings
from src.logger import log


def _with_docker():
    client = docker.from_env()
    return client


def build_image(repo_path, image_name):
    log(f"Building image {image_name}")
    client = _with_docker()
    try:
        client.images.build(path=repo_path, tag=image_name)
    except Exception as e:
        raise Exception(f"Error building image {image_name}: {e}")


def push_and_remove_image(image_name):
    log(f"Pushing image {image_name}")
    client = _with_docker()
    regSettings = get_settings()["registry"]

    url = regSettings.get("url")
    username = regSettings.get("username")
    password = regSettings.get("password")

    client = _with_docker()
    try:
        client.login(username=username, password=password, registry=url,
                     dockercfg_path="/home/pierrelf/kthcloud-secure-builds/config/config.json")
    except Exception as e:
        raise Exception(
            f"Error in login to registry {image_name}: {e}")

    try:
        client.images.push(image_name)
    except Exception as e:
        raise Exception(f"Error pushing image {image_name}: {e}")

    try:
        client.images.remove(image_name)
    except:
        pass

    remove_dangling()


def remove_dangling():
    client = _with_docker()

    for image in client.images.list():
        if image.tags == None:
            client.images.remove(image.id)
