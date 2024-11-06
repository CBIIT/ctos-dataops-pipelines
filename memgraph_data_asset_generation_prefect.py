from prefect import flow
from typing import Literal
from neo4j_summary import neo4j_summary
from neo4j_summary_prefect import create_mark_down
from data_model_archiving import data_model_archiving
from prefect.artifacts import create_markdown_artifact
from memgraph_export import memgraph_export
from bento.common.secret_manager import get_secret
from bento.common.utils import get_time_stamp, get_logger, LOG_PREFIX, APP_NAME
import yaml
import os
import prefect.variables as Variable

MEMGRAPH_HOST = "memgraph_host"
MEMGRAPH_USER = "memgraph_user"
MEMGRAPH_PASSWORD = "memgraph_password"
SUMARY_SECRET = "memgraph_summary_secret"
MEMGRAPH_PORT = "7687"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Memgraph Data Asset Generation'
    os.environ[APP_NAME] = 'Memgraph Data Asset Generation'

config_file = "config/prefect_drop_down_config_memgraph.yaml"
with open(config_file, 'r') as file:
    config = yaml.safe_load(file)
environment_choices = Literal[tuple(list(config.keys()))]

@flow(name="memgraph data asset generation", log_prints=True)
def memgraph_data_asset_generation_prefect(
    environment: environment_choices, # type: ignore
    s3_folder,
    s3_bucket,
    tmp_folder,
    data_model_repo_url,
    data_model_version,
    memgraph_summary_file_name,
    memgraph_dump_file_name
):
    log = get_logger('Memgraph Data Asset Generation')
    memgraph_secret = Variable.get(config[environment][SUMARY_SECRET])
    timestamp = get_time_stamp()
    if s3_folder == None or s3_folder == "":
        s3_folder = "neo4j-assets-" + timestamp
    secret = get_secret(memgraph_secret)
    memgraph_host = secret[MEMGRAPH_HOST]
    memgraph_user = secret[MEMGRAPH_USER]
    memgraph_password = secret[MEMGRAPH_PASSWORD]
    #generate memgraph summary
    memgraph_dict = neo4j_summary(memgraph_host, memgraph_user, memgraph_password, memgraph_summary_file_name, s3_bucket, s3_folder)
    summary_md = create_mark_down(memgraph_dict)
    create_markdown_artifact(
        key="Memgraph-summary",
        markdown=summary_md,
        description="Memgraph Summary",
    )
    #generate memgraph data model
    data_model_archiving(data_model_repo_url, data_model_version, s3_bucket, s3_folder)
    #generate memgraph database dump file
    memgraph_export(memgraph_host, MEMGRAPH_PORT, memgraph_user, memgraph_password, tmp_folder, s3_bucket, s3_folder, memgraph_dump_file_name, log)

if __name__ == "__main__":
    # create your first deployment
   memgraph_data_asset_generation_prefect.serve(name="memgraph_export")