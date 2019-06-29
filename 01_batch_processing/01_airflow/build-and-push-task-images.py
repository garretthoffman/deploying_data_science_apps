import docker
import boto3
import os
import sys
import base64
import logging
import json
import argparse

REMOTE_REPO_PREFIX = '161833574765.dkr.ecr.us-east-1.amazonaws.com/'
TASK_DIR = os.path.join(os.getcwd(), 'tasks/')

# configure logger
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
LOGGER = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--dag_id', type=str)
    parser.add_argument('--version', type=str, default='latest')
    parser.add_argument('--profile', type=str, default='default')
    parser.add_argument('--region', type=str, default='us-east-1')

    return parser.parse_known_args()


def build_and_push_dag_task_images(dag_id, version, profile, region):
    # Set up a session
    session = boto3.Session(profile_name=profile, region_name=region)
    ecr = session.client('ecr')

    # first get auth token to allow docker to push to ECR
    auth = ecr.get_authorization_token()
    token = auth["authorizationData"][0]["authorizationToken"]
    username, password = base64.b64decode(token).decode('utf-8').split(':')
    endpoint = auth["authorizationData"][0]["proxyEndpoint"]
    auth_config_payload = {'username': username, 'password': password}

    # walk down intothe DAG directory to get the task names from their directories
    dag_directory = os.path.join(TASK_DIR, dag_id)
    directory_walk = os.walk(dag_directory)
    _, tasks, _ = next(directory_walk)

    for task_id in tasks:
        build_and_push_task_image(dag_id, task_id, version, ecr, auth_config_payload)


def build_and_push_task_image(dag_id, task_id, version, ecr_client, auth_config_payload):
    # The ECR Repo Name
    repo_name = dag_id + "-" + task_id
    # The ECR Repository URI
    repo_uri = REMOTE_REPO_PREFIX + repo_name

    # create_repo if doesn't exist
    LOGGER.info(f"Checking {repo_uri} exists")
    remote_repo_data = ecr_client.describe_repositories(maxResults=1000)
    remote_repos = {repo['repositoryName'] for repo in remote_repo_data['repositories']}
    if repo_name not in remote_repos:
        LOGGER.info(f"Creating {repo_uri}")
        ecr_client.create_repository(repositoryName=repo_name)

    # How you want to tag your project locally
    local_tag = dag_id + "-" + task_id

    # Create Docker Client
    docker_api = docker.APIClient()

    LOGGER.info(f"Building image {local_tag}")
    path = os.path.join(TASK_DIR, dag_id, task_id)
    for line in docker_api.build(path=path, tag=local_tag, dockerfile='./Dockerfile'):
        process_docker_build_api_line(line)

    version_tag = repo_uri + ':' + version

    LOGGER.info(f"Tagging version {version_tag}")
    if docker_api.tag(local_tag, version_tag) is False:
        raise RuntimeError("Tag appeared to fail: " + version_tag)

    LOGGER.info(f"Pushing to repo {version_tag}")
    for line in docker_api.push(version_tag, stream=True, auth_config=auth_config_payload):
        process_docker_push_api_line(line)


    LOGGER.info(f"Removing taged deployment images")
    # You will still have the local_tag image if you need to troubleshoot
    docker_api.remove_image(version_tag, force=True)

def process_docker_build_api_line(payload):
    """ Process the output from API stream, throw an Exception if there is an error """
    # Sometimes Docker sends two "{}\n" blocks together...
    for segment in payload.decode('utf-8').split('\n'):
        line = segment.strip()
        if line:
            try:
                line_payload = json.loads(line)
            except ValueError as ex:
                LOGGER.info(f"Could not decipher payload from API: {ex.message}")
            if line_payload:
                if "errorDetail" in line_payload:
                    error = line_payload["errorDetail"]
                    sys.stderr.write(error["message"])
                    raise RuntimeError("Error on build - code " + error["code"])
                elif "stream" in line_payload:
                    sys.stdout.write(line_payload["stream"])

def process_docker_push_api_line(payload):
    """ Process the output from API stream, throw an Exception if there is an error """
    # Sometimes Docker sends two "{}\n" blocks together...
    for segment in payload.decode('utf-8').split('\n'):
        line = segment.strip()
        if line:
            try:
                line_payload = json.loads(line)
            except ValueError as ex:
                LOGGER.info(f"Could not decipher payload from API: {ex.message}")
            if line_payload:
                if "errorDetail" in line_payload:
                    error = line_payload["errorDetail"]
                    sys.stderr.write(error["message"])
                    raise RuntimeError("Error on push: " + error["message"])
                elif "status" in line_payload and "id" in line_payload:
                    output = line_payload["id"] + " : " + line_payload["status"] + "\n"
                    sys.stdout.write(output)

if __name__ == '__main__':
    args, _ = parse_args()
    build_and_push_dag_task_images(args.dag_id, args.version, args.profile, args.region)