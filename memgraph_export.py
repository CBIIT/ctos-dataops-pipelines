import subprocess
import os
import docker
import yaml
import argparse
from bento.common.utils import get_time_stamp, get_logger, LOG_PREFIX, APP_NAME
from bento.common.s3 import upload_log_file
import sys

MEMGRAPH_HOST = "memgraph_host"
MEMGRAPH_PORT = "memgraph_port"
MEMGRAPH_USERNAME = "memgraph_username"
MEMGRAPH_PASSWORD = "memgraph_password"
TEMP_FOLDER = "tmp_folder"
LOCAL_TEMP_FOLDER = "local_tmp_folder"
COMPOSE_FILE_PATH = "compose_file_path"
SERVICES = "services"
MEMGRAPH = "memgraph"
CONTAINER_NAME = "container_name"
S3_BUCKET = "s3_bucket"
S3_PREFIX = "s3_prefix"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Memgraph_Export'
    os.environ[APP_NAME] = 'Memgraph_Export'


def run_docker_compose(compose_file, log):
    # Run the command `docker-compose up -d` to start the containers
    try:
        subprocess.run(["docker-compose", "-f", compose_file, "up", "-d"], check=True)
        log.info("Docker Compose up: Containers are running.")
    except Exception as e:
        log.error(f"Error running docker-compose up: {e}")

def upload_s3(s3_prefix, s3_bucket, file_key, log):
    dest = os.path.join(f"s3://{s3_bucket}", s3_prefix)
    upload_log_file(dest, file_key)
    log.info(f'Uploading memgraph export file {os.path.basename(file_key)} succeeded!')

def stop_docker_compose(compose_file_path, log):
    # Run the command `docker-compose down` to stop and remove the containers
    try:
        subprocess.run(["docker-compose", "-f", compose_file_path, "down"], check=True)
        log.info("Docker Compose down: Containers stopped and removed.")
    except Exception as e:
        print(f"Error running docker-compose down: {e}")

def memgraph_export(container_name, memgraph_host, memgraph_port, memgraph_username, memgraph_password, tmp_folder, local_tmp_folder, s3_bucket, s3_prefix, log):
    try:
        client = docker.from_env()
        container = client.containers.get(container_name)
        if container.status == "running":
            export_filename = "memgraph_export-" + get_time_stamp() + ".cypherl"
            export_file_key = os.path.join(tmp_folder, export_filename)
            command = [
                "sh",
                "-c",
                f'echo "DUMP DATABASE;" | mgconsole --host {memgraph_host} --port {memgraph_port} --username {memgraph_username} --password {memgraph_password} --output-format=cypherl > {export_file_key}'
                ]
            exit_code, output = container.exec_run(command)
            if exit_code == 0:
                local_file_key =  os.path.join(local_tmp_folder, export_filename)
                log.info(f"Memgraph export file was created and stored in {local_file_key}")
                upload_s3(s3_prefix, s3_bucket, local_file_key, log)
            else:
                log.error(output)
                sys.exit(1)
                

        else:
            log.error(f"Container Status: {container.status}")
            sys.exit(1)
    except Exception as e:
        log.error(e)

def memgraph_export_local(memgraph_export_config_file):
    with open(memgraph_export_config_file, "r") as file:
        memgraph_export_config = yaml.safe_load(file)
    compose_file_path = memgraph_export_config[COMPOSE_FILE_PATH]
    memgraph_host = memgraph_export_config[MEMGRAPH_HOST]
    memgraph_port = memgraph_export_config[MEMGRAPH_PORT]
    memgraph_username = memgraph_export_config[MEMGRAPH_USERNAME]
    memgraph_password = memgraph_export_config[MEMGRAPH_PASSWORD]
    local_tmp_folder = memgraph_export_config[LOCAL_TEMP_FOLDER]
    tmp_folder = memgraph_export_config[TEMP_FOLDER]
    s3_bucket = memgraph_export_config[S3_BUCKET]
    s3_prefix = memgraph_export_config[S3_PREFIX]
    log = get_logger('Memgraph_Export')

    with open(compose_file_path, 'r') as file:
        compose_file = yaml.safe_load(file)
    container_name = compose_file[SERVICES][MEMGRAPH][CONTAINER_NAME]
    run_docker_compose(compose_file_path, log)
    memgraph_export(container_name, memgraph_host, memgraph_port, memgraph_username, memgraph_password, tmp_folder, local_tmp_folder, s3_bucket, s3_prefix, log)
    stop_docker_compose(compose_file_path, log)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Memgraph database backup')
    parser.add_argument('config_file', help='Confguration file', nargs='?', default=None)
    args = parser.parse_args()
    config_file = args.config_file
    memgraph_export_local(config_file)
