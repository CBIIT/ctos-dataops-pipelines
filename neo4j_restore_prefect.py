from prefect import flow
import os
import sys
import json
from bento.common.secret_manager import get_secret
from neo4j_restore import neo4j_restore, downlaod_s3
from neo4j_summary import neo4j_summary
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME

NEO4J_IP = "neo4j_ip"
NEO4J_USER = "neo4j_user"
NEO4J_KEY = "neo4j_key"
NEO4J_PASSWORD = "neo4j_password"
TMP = "/tmp/"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Neo4j_Restore'
    os.environ[APP_NAME] = 'Neo4j_Restore'

@flow(name="neo4j restore", log_prints=True)
def neo4j_restore_prefect(
        neo4j_restore_secrect,
        neo4j_summary_secret,
        s3_bucket,
        s3_file_key,
        s3_summary_file_key,
        neo4j_summary_folder,
        neo4j_summary_file_name
):  
    log = get_logger('Neo4j Restore')
    secret = get_secret(neo4j_summary_secret)
    secret_ssh = get_secret(neo4j_restore_secrect)
    neo4j_ip = secret[NEO4J_IP]
    neo4j_user = secret_ssh[NEO4J_USER]
    neo4j_key = secret_ssh[NEO4J_KEY]
    neo4j_restore(neo4j_ip, neo4j_user, neo4j_key, s3_bucket, s3_file_key)

    neo4j_summary_user = secret[NEO4J_USER]
    neo4j_summary_password = secret[NEO4J_PASSWORD]
    restore_neo4j_summary = neo4j_summary(neo4j_ip, neo4j_summary_user, neo4j_summary_password, neo4j_summary_file_name, s3_bucket, neo4j_summary_folder)
    summary_file_key = os.path.join(TMP, os.path.basename(s3_summary_file_key))
    downlaod_s3(s3_bucket, s3_summary_file_key, log, summary_file_key)
    with open(summary_file_key, 'r') as file:
        compare_neo4j_summary = json.load(file)
    if restore_neo4j_summary == compare_neo4j_summary:
        log.info("Data restore successfully")
    else:
        log.error("Data resotre fail")
        sys.exit(1)

if __name__ == "__main__":
    # create your first deployment
   neo4j_restore_prefect.serve(name="neo4j_restore")

