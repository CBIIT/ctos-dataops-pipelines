from prefect import flow
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from bento.common.secret_manager import get_secret
import os
import prefect.variables as Variable
from opensearch_backup import opensearch_backup
import boto3

SUMARY_SECRET = "memgraph_summary_secret"
ES_HOST = "es_host"
#PROJECT_NAME  = "gen"
REGION = "us-east-1"
ENVIRONMENT = "env"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'OpenSearch Backup'
    os.environ[APP_NAME] = 'OpenSearch Backup'


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
    snapshot_name,
    secret_name_prefect_variable,
    aws_role_prefect_variable,
    opensearch_repo,
    s3_bucket,
    indices
):
    log = get_logger('OpenSearch Backup')
    opensearch_secret = Variable.get(secret_name_prefect_variable)
    secret = get_secret(opensearch_secret)
    snapshot_role = (
        aws_role_prefect_variable
        if aws_role_prefect_variable.startswith("arn:")
        else Variable.get(aws_role_prefect_variable)
    )
    if snapshot_role.startswith("arn:"):
        role_arn = snapshot_role
    else:
        aws_account_id = get_aws_account_id(log)
        role_arn = f"arn:aws:iam::{aws_account_id}:role/{snapshot_role}"
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
    opensearch_backup(argList)

if __name__ == "__main__":
    # create your first deployment
   opensearch_backup_prefect.serve(name="opensearch_backup")