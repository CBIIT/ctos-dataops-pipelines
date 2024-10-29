from prefect import flow
from neo4j_dump_prefect import neo4j_dump_prefect
from neo4j_summary_prefect import neo4j_secret_summary_prefect
from data_model_archiving_prefect import data_model_archiving_prefect
from bento.common.utils import get_time_stamp
import prefect.variables as Variable
from typing import Literal
import yaml

config_file = "config/prefect_drop_down_config.yaml"
with open(config_file, 'r') as file:
    config = yaml.safe_load(file)
environment_choices = Literal[tuple(list(config.keys()))]
SUMARY_SECRET = "neo4j_summary_secret"
DUMP_SECRET = "neo4j_ssh_secret"

@flow(name="data asset generation", log_prints=True)
def data_asset_generation_prefect(
        environment: environment_choices,
        data_model_version,
        s3_folder,
        neo4j_summary_file_name,
        neo4j_dump_file_name,
        data_model_repo_url,
        s3_bucket
    ):
    neo4j_summary_secret = Variable.get(config[environment][SUMARY_SECRET])
    neo4j_dump_secret = Variable.get(config[environment][DUMP_SECRET])
    timestamp = get_time_stamp()
    if s3_folder == None or s3_folder == "":
        s3_folder = "neo4j-assets-" + timestamp
    neo4j_secret_summary_prefect(neo4j_summary_secret, s3_bucket, s3_folder, neo4j_summary_file_name)
    data_model_archiving_prefect(data_model_repo_url, data_model_version, s3_bucket, s3_folder)
    neo4j_dump_prefect(neo4j_dump_secret, neo4j_summary_secret, s3_bucket, s3_folder, neo4j_dump_file_name)

if __name__ == "__main__":
    # create your first deployment
    data_asset_generation_prefect.serve(name="data_asset_generation")