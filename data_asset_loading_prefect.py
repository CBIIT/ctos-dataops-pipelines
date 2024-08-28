from prefect import flow
import os
import sys
import json
from bento.common.secret_manager import get_secret
from neo4j_restore import neo4j_restore, downlaod_s3
from neo4j_summary import neo4j_summary
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from bento.common.utils import get_time_stamp
import prefect.variables as Variable
from typing import Literal

NEO4J_IP = "neo4j_ip"
NEO4J_USER = "neo4j_user"
NEO4J_KEY = "neo4j_key"
NEO4J_PASSWORD = "neo4j_password"
TMP = "/tmp/"
environment_choices = Literal["dev", "dev2", "qa", "qa2"]
if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Neo4j_Restore'
    os.environ[APP_NAME] = 'Neo4j_Restore'

@flow(name="data asset loading", log_prints=True)
def data_asset_loading_prefect(
        environment: environment_choices,
        s3_bucket,
        s3_folder,
        dump_file_name,
        validation_summary_file_name,
        restore_summary_file_name
):  
    log = get_logger('Neo4j Data Asset Loading')
    
    neo4j_restore_secrect = ""
    neo4j_summary_secret = ""
    if environment == "dev":
        neo4j_summary_secret =  Variable.get('cds_secret_name_dev')
        neo4j_restore_secrect = Variable.get('cds_secret_name_ssh')
    elif environment == "dev2":
        neo4j_summary_secret = Variable.get('cds_secret_name_dev2')
        neo4j_restore_secrect = Variable.get('cds_secret_name_ssh')
    elif environment == "qa":
        neo4j_summary_secret = Variable.get('cds_secret_name_qa')
        neo4j_restore_secrect = Variable.get('cds_secret_name_ssh')
    elif environment == "qa2":
        neo4j_summary_secret = Variable.get('cds_secret_name_qa2')
        neo4j_restore_secrect = Variable.get('cds_secret_name_ssh')
    
    secret = get_secret(neo4j_summary_secret)
    secret_ssh = get_secret(neo4j_restore_secrect)
    neo4j_ip = secret[NEO4J_IP]
    print(neo4j_summary_secret)
    print(neo4j_ip)
    neo4j_user = secret_ssh[NEO4J_USER]
    neo4j_key = secret_ssh[NEO4J_KEY]
    s3_dump_file_key = os.path.join(s3_folder, dump_file_name)
    s3_summary_file_key = os.path.join(s3_folder, validation_summary_file_name)
    neo4j_restore(neo4j_ip, neo4j_user, neo4j_key, s3_bucket, s3_dump_file_key)

    neo4j_summary_user = secret[NEO4J_USER]
    neo4j_summary_password = secret[NEO4J_PASSWORD]
    restore_neo4j_summary = neo4j_summary(neo4j_ip, neo4j_summary_user, neo4j_summary_password, restore_summary_file_name, s3_bucket, s3_folder)
    summary_file_key = os.path.join(TMP, os.path.basename(s3_summary_file_key))
    downlaod_s3(s3_bucket, s3_summary_file_key, log, summary_file_key)
    with open(summary_file_key, 'r') as file:
        compare_neo4j_summary = json.load(file)
    if restore_neo4j_summary == compare_neo4j_summary:
        log.info("Data asset loading successfully")
    else:
        log.error("Data asset loading fail")
        sys.exit(1)

if __name__ == "__main__":
    # create your first deployment
   data_asset_loading_prefect.serve(name="neo4j_restore")

