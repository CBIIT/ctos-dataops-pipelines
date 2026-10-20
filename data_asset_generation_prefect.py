from prefect import flow
from datetime import datetime
import prefect.variables as Variable
from typing import Literal
import yaml

config_file = "config/prefect_drop_down_config.yaml"
with open(config_file, 'r') as file:
    config = yaml.safe_load(file)
environment_choices = Literal[tuple(list(config.keys()))]
prefect_config_file = "config/ins-prefect.yaml"
with open(prefect_config_file, 'r') as file:
    prefect_config = yaml.safe_load(file) or {}
data_model_repo_url_default = (
    prefect_config.get("data_model_repo_url")
    or "https://github.com/CBIIT/ins-model"
)
default_file_timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
neo4j_summary_file_name_default = f"DevDump_{default_file_timestamp}.json"
neo4j_dump_file_name_default = f"DevDump_{default_file_timestamp}.dump"
SUMARY_SECRET = "neo4j_summary_secret"
DUMP_SECRET = "neo4j_ssh_secret"

@flow(name="data asset generation", log_prints=True)
def data_asset_generation_prefect(
        environment: environment_choices, # type: ignore
        data_model_version: str,
        s3_bucket: str,
        s3_folder: str="dump_files",
        neo4j_summary_file_name: str=neo4j_summary_file_name_default,
        neo4j_dump_file_name: str=neo4j_dump_file_name_default,
        data_model_repo_url: str=data_model_repo_url_default,
    ):
    """Back up Neo4j and archive the selected model from its remote repository.

    Args:
        data_model_version: Model branch or tag, such as 2.5.0. Fetched fresh
            on every run; new branches need no deployment update.
    """
    from data_model_archiving_prefect import data_model_archiving_prefect
    from neo4j_dump_prefect import neo4j_dump_prefect
    from neo4j_summary_prefect import neo4j_secret_summary_prefect

    neo4j_summary_secret = Variable.get(config[environment][SUMARY_SECRET])
    neo4j_dump_secret = Variable.get(config[environment][DUMP_SECRET])
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    if not s3_folder:
        s3_folder = "dump_files"
    if not neo4j_summary_file_name:
        neo4j_summary_file_name = f"DevDump_{timestamp}.json"
    if not neo4j_dump_file_name:
        neo4j_dump_file_name = f"DevDump_{timestamp}.dump"
    neo4j_secret_summary_prefect(neo4j_summary_secret, s3_bucket, s3_folder, neo4j_summary_file_name)
    data_model_archiving_prefect(data_model_repo_url, data_model_version, s3_bucket, s3_folder)
    neo4j_dump_prefect(neo4j_dump_secret, neo4j_summary_secret, s3_bucket, s3_folder, neo4j_dump_file_name)

if __name__ == "__main__":
    # create your first deployment
    data_asset_generation_prefect.serve(name="data_asset_generation")
