from prefect import flow
from typing import Literal
import yaml
import re
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from get_ARN import get_secret_ARN
import os
import prefect.variables as Variable
from opensearch_backup import opensearch_backup
import boto3

SUMARY_SECRET = "memgraph_summary_secret"
ES_HOST = "es_host"
PROJECT_NAME  = "ctdc"
REGION = "us-east-1"
ENVIRONMENT = "env"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'OpenSearch Backup'
    os.environ[APP_NAME] = 'OpenSearch Backup'

config_file = "config/prefect_drop_down_config_memgraph.yaml"
with open(config_file, 'r') as file:
    config = yaml.safe_load(file)
environment_choices = Literal[tuple(list(config.keys()))]

config_file_for_accounts = "config/prefect_account_config.yaml"
with open(config_file_for_accounts, 'r') as file:
    account_config = yaml.safe_load(file)
account_choices = Literal[tuple(list(account_config.keys()))]

def get_role_arn_from_caller_identity():
    # STS returns: arn:aws:sts::ACCOUNT:assumed-role/ROLE_NAME/SESSION
    # OpenSearch needs: arn:aws:iam::ACCOUNT:role/ROLE_NAME
    sts_client = boto3.client("sts")
    caller_arn = sts_client.get_caller_identity()["Arn"]
    match = re.match(r'arn:aws:sts::(\d+):assumed-role/([^/]+)/', caller_arn)
    if match:
        account_id, role_name = match.group(1), match.group(2)
        return f"arn:aws:iam::{account_id}:role/{role_name}"
    return caller_arn


@flow(name="OpenSearch backup", log_prints=True)
def opensearch_backup_prefect(
    environment: environment_choices, # type: ignore
    snapshot_name,
    indices,
    account_number,
    s3_bucket
):
    log = get_logger('OpenSearch Backup')
    opensearch_secret = Variable.get(config[environment][SUMARY_SECRET])
    secret = get_secret_ARN(opensearch_secret, account_number)
    role_arn = get_role_arn_from_caller_identity()
    argList = {
        'oshost': "https://" + secret[ES_HOST] + "/",
        'repo': PROJECT_NAME,
        's3bucket': s3_bucket,
        'snapshot': snapshot_name,
        'indices': indices,
        'role_arn': role_arn,
        'region': REGION,
        'basepath': snapshot_name
    }
    opensearch_backup(argList)

if __name__ == "__main__":
    # create your first deployment
   opensearch_backup_prefect.serve(name="opensearch_backup")