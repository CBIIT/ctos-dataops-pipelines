from prefect import flow
from typing import Literal
import yaml
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from bento.common.secret_manager import get_secret
import os
import prefect.variables as Variable
from opensearch_restore import opensearch_restore
from opensearch_backup_prefect import get_aws_account_id
import boto3

SUMARY_SECRET = "memgraph_summary_secret"
ES_HOST = "es_host"
REGION = "us-east-1"
ENVIRONMENT = "env"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'OpenSearch Restore'
    os.environ[APP_NAME] = 'OpenSearch Restore'

config_file = "config/prefect_drop_down_config_memgraph.yaml"
with open(config_file, 'r') as file:
    config = yaml.safe_load(file)
environment_choices = Literal[tuple(list(config.keys()))]

@flow(name="OpenSearch restore", log_prints=True)
def opensearch_restore_prefect(
    snapshot_name,
    secret_name_prefect_variable,
    aws_role_prefect_variable,
    opensearch_repo,
    indices,
    s3_bucket
):
    log = get_logger('OpenSearch Restore')
    opensearch_secret = Variable.get(secret_name_prefect_variable)
    secret = get_secret(opensearch_secret)
    aws_account_id = get_aws_account_id(log)
    role_arn = f"arn:aws:iam::{aws_account_id}:role/"+ Variable.get(aws_role_prefect_variable)
    argList = {
        'oshost': "https://" + secret[ES_HOST] + "/",
        'repo': opensearch_repo,
        's3bucket': s3_bucket,
        'snapshot': snapshot_name,
        'indices': indices,
        'rolearn': role_arn,
        'region': REGION,
        'basepath': snapshot_name
    }
    opensearch_restore(argList)

if __name__ == "__main__":
    # create your first deployment
   opensearch_restore_prefect.serve(name="opensearch_backup")