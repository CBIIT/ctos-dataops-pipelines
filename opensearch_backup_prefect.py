from prefect import flow
from typing import Literal
import yaml
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from bento.common.secret_manager import get_secret
import os
import prefect.variables as Variable
from opensearch_backup import opensearch_backup

SUMMARY_SECRET = "memgraph_summary_secret"
ES_HOST = "ES_HOST"
PROJECT_NAME  = "popsci"
REGION = "us-east-1"
ENVIRONMENT = "env"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'OpenSearch Backup'
    os.environ[APP_NAME] = 'OpenSearch Backup'

config_file = "config/prefect_drop_down_config_memgraph.yaml"
with open(config_file, 'r') as file:
    config = yaml.safe_load(file)
environment_choices = Literal[tuple(list(config.keys()))]

@flow(name="OpenSearch backup", log_prints=True)
def opensearch_backup_prefect(
    environment: environment_choices, # type: ignore
    snapshot_name,
    indices,
    s3_bucket
):
    log = get_logger('OpenSearch Backup')
    opensearch_secret = Variable.get(config[environment][SUMMARY_SECRET])
    secret = get_secret(opensearch_secret)
    role_arn = Variable.get("popsci_role_arn")
    os_role_arn = Variable.get("popsci_os_role_arn")
    argList = {
        'oshost': "https://" + secret[ES_HOST] + "/",
        'repo': PROJECT_NAME,
        's3bucket': s3_bucket,
        'snapshot': snapshot_name,
        'indices': indices,
        'rolearn': role_arn,
        'osrolearn': os_role_arn,
        'region': REGION,
        'basepath': snapshot_name
    }
    opensearch_backup(argList)

if __name__ == "__main__":
    # create your first deployment
   opensearch_backup_prefect.serve(name="opensearch_backup")