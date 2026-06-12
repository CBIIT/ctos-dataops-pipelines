from prefect import flow
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from bento.common.secret_manager import get_secret
import os
import prefect.variables as Variable
from opensearch_backup import opensearch_backup
import boto3

ES_HOST = "es_host"
ROLE_ARN = "opensearch_role_arn"
REGION = "us-east-1"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'OpenSearch Backup'
    os.environ[APP_NAME] = 'OpenSearch Backup'


def assume_role(role_arn, session_name="opensearch-backup"):
    """Assume the specified role and return a session with temporary credentials"""
    sts_client = boto3.client('sts')
    response = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name
    )
    
    credentials = response['Credentials']
    assumed_session = boto3.Session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
    return assumed_session


@flow(name="OpenSearch backup", log_prints=True)
def opensearch_backup_prefect(
    snapshot_name,
    secret_name_prefect_variable,
    opensearch_repo,
    s3_bucket,
    indices,
    aws_role_prefect_variable=None
):
    log = get_logger('OpenSearch Backup')
    opensearch_secret = Variable.get(secret_name_prefect_variable)
    secret = get_secret(opensearch_secret)
    
    # Get role ARN from secret
    role_arn = secret.get(ROLE_ARN)
    if not role_arn:
        raise ValueError(f"Role ARN not found in secret {opensearch_secret}")
    
    # Assume the role to get temporary credentials
    assumed_session = assume_role(role_arn)
    
    argList = {
        'oshost': "https://" + secret[ES_HOST] + "/",
        'repo': opensearch_repo,
        's3bucket': s3_bucket,
        'snapshot': snapshot_name,
        'indices': indices,
        'region': REGION,
        'basepath': snapshot_name
    }
    opensearch_backup(argList, assumed_session)

if __name__ == "__main__":
    # create your first deployment
   opensearch_backup_prefect.serve(name="opensearch_backup")