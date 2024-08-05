from prefect import flow
from neo4j_dump_prefect import neo4j_dump_prefect
from neo4j_summary_prefect import neo4j_secret_summary_prefect
from data_model_archiving_prefect import data_model_archiving_prefect
from bento.common.utils import get_time_stamp
import prefect.variables as Variable
from typing import Literal

environment_choices = Literal["dev", "dev2", "qa", "qa2"]
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
    if environment == "dev":
        neo4j_summary_secret = Variable.get('cds_secret_name_dev')
        neo4j_dump_secret = Variable.get('cds_secret_name_ssh')
    elif environment == "dev2":
        neo4j_summary_secret = Variable.get('cds_secret_name_dev2')
        neo4j_dump_secret = Variable.get('cds_secret_name_ssh')
    elif environment == "qa":
        neo4j_summary_secret = Variable.get('cds_secret_name_qa')
        neo4j_dump_secret = Variable.get('cds_secret_name_ssh')
    elif environment == "qa2":
        neo4j_summary_secret = Variable.get('cds_secret_name_qa2')
        neo4j_dump_secret = Variable.get('cds_secret_name_ssh')
    timestamp = get_time_stamp()
    if s3_folder == None or s3_folder == "":
        s3_folder = "neo4j-assets-" + timestamp
    neo4j_secret_summary_prefect(neo4j_summary_secret, s3_bucket, s3_folder, neo4j_summary_file_name)
    data_model_archiving_prefect(data_model_repo_url, data_model_version, s3_bucket, s3_folder)
    neo4j_dump_prefect(neo4j_dump_secret, neo4j_summary_secret, s3_bucket, s3_folder, neo4j_dump_file_name)

if __name__ == "__main__":
    # create your first deployment
    data_asset_generation_prefect.serve(name="data_asset_generation")