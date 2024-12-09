from prefect import flow
from typing import Literal
from neo4j_summary import neo4j_summary
from memgraph_restore import memgraph_restore
from bento.common.secret_manager import get_secret
from neo4j_restore import downlaod_s3
from bento.common.utils import get_time_stamp, get_logger, LOG_PREFIX, APP_NAME
import yaml
import os
import prefect.variables as Variable
import json
import sys
import subprocess

MEMGRAPH_HOST = "memgraph_host"
MEMGRAPH_USER = "memgraph_user"
MEMGRAPH_PASSWORD = "memgraph_password"
SUMARY_SECRET = "memgraph_summary_secret"
MEMGRAPH_PORT = "7687"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Memgraph Data Asset Loading'
    os.environ[APP_NAME] = 'Memgraph Data Asset Loading'

config_file = "config/prefect_drop_down_config_memgraph.yaml"
with open(config_file, 'r') as file:
    config = yaml.safe_load(file)
environment_choices = Literal[tuple(list(config.keys()))]

@flow(name="memgraph data asset loading", log_prints=True)
def memgraph_data_asset_loading_prefect(
    environment: environment_choices, # type: ignore
    s3_folder,
    s3_bucket,
    tmp_folder,
    memgraph_validation_summary_file_name,
    memgraph_restore_summary_file_name,
    memgraph_dump_file_name
):
    log = get_logger('Memgraph Data Asset Loading')
    memgraph_secret = Variable.get(config[environment][SUMARY_SECRET])
    timestamp = get_time_stamp()
    if s3_folder == None or s3_folder == "":
        s3_folder = "memgraph-assets-" + timestamp
    secret = get_secret(memgraph_secret)
    memgraph_host = secret[MEMGRAPH_HOST]
    memgraph_user = secret[MEMGRAPH_USER]
    memgraph_password = secret[MEMGRAPH_PASSWORD]
    #generate memgraph database dump file
    memgraph_restore(memgraph_host, MEMGRAPH_PORT, memgraph_user, memgraph_password, tmp_folder, s3_bucket, s3_folder, memgraph_dump_file_name, log)
    #validate the memgraph restoring
    memgraph_restore_summary = neo4j_summary(memgraph_host, memgraph_user, memgraph_password, memgraph_restore_summary_file_name, s3_bucket, s3_folder)
    s3_validation_file_key = os.path.join(s3_folder, memgraph_validation_summary_file_name)
    validation_file_key = os.path.join(tmp_folder, memgraph_validation_summary_file_name)
    downlaod_s3(s3_bucket, s3_validation_file_key, log, validation_file_key)
    with open(validation_file_key, 'r') as file:
        memgraph_validation_summary = json.load(file)
        log.info(memgraph_validation_summary)
    if memgraph_restore_summary == memgraph_validation_summary:
        log.info("Data asset loading successfully")
    else:
        log.error("Data asset counting unmatched")
        log.error("Data asset loading fail")
        sys.exit(1)

if __name__ == "__main__":
    # create your first deployment
   memgraph_data_asset_loading_prefect.serve(name="memgraph_restore")