from prefect import flow
from datetime import datetime
from github_refs import get_github_refs
import prefect.variables as Variable
from typing import Literal, Optional
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
data_model_versions = get_github_refs(
    data_model_repo_url_default,
    include_tags=True,
)
data_model_version_choices = (
    Literal[tuple(data_model_versions)]
    if data_model_versions
    else str
)
SUMARY_SECRET = "neo4j_summary_secret"
DUMP_SECRET = "neo4j_ssh_secret"

@flow(name="data asset generation", log_prints=True)
def data_asset_generation_prefect(
        environment: environment_choices, # type: ignore
        data_model_version: data_model_version_choices, # type: ignore
        s3_bucket: str,
        s3_folder: str="dump_files",
        neo4j_summary_file_name: Optional[str]=None,
        neo4j_dump_file_name: Optional[str]=None,
        data_model_repo_url: str=data_model_repo_url_default,
    ):
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
