from prefect import flow
from typing import Literal
import yaml
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from bento.common.secret_manager import get_secret
import os
import prefect.variables as Variable
from opensearch_backup import opensearch_backup
import boto3

SUMARY_SECRET = "memgraph_summary_secret"
ES_HOST = "es_host"
PROJECT_NAME  = "gen"
REGION = "us-east-1"
ENVIRONMENT = "env"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'OpenSearch Backup'
    os.environ[APP_NAME] = 'OpenSearch Backup'

config_file = "config/prefect_drop_down_config_memgraph.yaml"
with open(config_file, 'r') as file:
    config = yaml.safe_load(file)
environment_choices = Literal[tuple(list(config.keys()))]

def get_aws_account_id(log):
    try:
        sts_client = boto3.client("sts")
        response = sts_client.get_caller_identity()
        account_id = response["Account"]
        return account_id
    except Exception as e:
        log.info(f"Error getting AWS account ID: {e}")
        return None



@flow(name="OpenSearch backup", log_prints=True)
def opensearch_backup_prefect(
    environment: environment_choices, # type: ignore
    snapshot_name,
    indices,
    s3_bucket
):
    log = get_logger('OpenSearch Backup')
    opensearch_secret = Variable.get(config[environment][SUMARY_SECRET])
    secret = get_secret(opensearch_secret)
    aws_account_id = get_aws_account_id(log)
    aws_account_env = Variable.get(config[environment][ENVIRONMENT])
    role_arn = f"arn:aws:iam::${aws_account_id}:role/power-user-crdc-${aws_account_env}-cds-opensearch-snapshot"
    argList = {
        'oshost': secret[ES_HOST],
        'repo': PROJECT_NAME,
        's3bucket': s3_bucket,
        'snapshot': snapshot_name,
        'indices': indices,
        'rolearn': role_arn,
        'region': REGION,
        'basepath': snapshot_name
    }
    opensearch_backup(argList)

if __name__ == "__main__":
    # create your first deployment
   opensearch_backup_prefect.serve(name="opensearch_backup")