from prefect import flow
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from bento.common.secret_manager import get_secret
import os
import prefect.variables as Variable
from opensearch_backup import opensearch_backup
import boto3
from typing import List, Literal
import yaml

SUMARY_SECRET = "memgraph_summary_secret"
INS_SECRET = "neo4j_summary_secret"
ES_HOST = "es_host"
#PROJECT_NAME  = "gen"
REGION = "us-east-1"
ENVIRONMENT = "env"
OPERATIONS_ROLE_VARIABLE = "aws_operations_role"
DEFAULT_OPERATIONS_ROLE_ARN = "arn:aws:iam::893402228433:role/ccdi-id-prod-prefect-operations"
INS_DROPDOWN_CONFIG_FILE = "config/prefect_drop_down_config.yaml"
INS_PREFECT_CONFIG_FILE = "config/ins-prefect.yaml"

with open(INS_DROPDOWN_CONFIG_FILE, 'r') as file:
    ins_dropdown_config = yaml.safe_load(file)
with open(INS_PREFECT_CONFIG_FILE, 'r') as file:
    ins_prefect_config = yaml.safe_load(file) or {}

environment_choices = Literal[tuple(ins_dropdown_config.keys())]

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



def resolve_role(role_or_variable, log):
  # Accepts a full ARN, a Prefect variable name, or a bare role name.
  if not role_or_variable:
    return ""
  role = role_or_variable if role_or_variable.startswith("arn:") else Variable.get(role_or_variable)
  if role.startswith("arn:"):
    return role
  return f"arn:aws:iam::{get_aws_account_id(log)}:role/{role}"


def resolve_operations_role(aws_operations_role, log):
  # Falls back to a Prefect variable, then a hardcoded default, so runs work even without run-time parameter delivery.
  if not aws_operations_role:
    try:
      aws_operations_role = Variable.get(OPERATIONS_ROLE_VARIABLE) or ""
    except Exception as e:
      log.info(f"No {OPERATIONS_ROLE_VARIABLE} Prefect variable found: {e}")
      aws_operations_role = ""
  if not aws_operations_role:
    aws_operations_role = DEFAULT_OPERATIONS_ROLE_ARN
  return resolve_role(aws_operations_role, log)


def run_opensearch_backup(
    snapshot_name,
    secret_name_prefect_variable,
    aws_role_prefect_variable,
    opensearch_repo,
    s3_bucket,
    indices,
    aws_operations_role,
):
    log = get_logger('OpenSearch Backup')
    opensearch_secret = Variable.get(secret_name_prefect_variable)
    secret = get_secret(opensearch_secret)
    role_arn = resolve_role(aws_role_prefect_variable, log)
    operations_role_arn = resolve_operations_role(aws_operations_role, log)
    print(f"snapshot role: {role_arn or '<empty>'}")
    print(f"operations role: {operations_role_arn or '<empty - using task credentials>'}")
    argList = {
        'oshost': "https://" + secret[ES_HOST] + "/",
        'repo': opensearch_repo,
        's3bucket': s3_bucket,
        'snapshot': snapshot_name,
        'indices': indices,
        'rolearn': role_arn,
        'operationsrolearn': operations_role_arn,
        'region': REGION,
        'basepath': snapshot_name
    }
    opensearch_backup(argList)

@flow(name="OpenSearch backup", log_prints=True)
def opensearch_backup_prefect(
    snapshot_name: str,
    secret_name_prefect_variable: str,
    aws_role_prefect_variable: str,
    opensearch_repo: str,
    s3_bucket: str,
    indices: str = "",
    aws_operations_role: str = "",
):
    run_opensearch_backup(
        snapshot_name,
        secret_name_prefect_variable,
        aws_role_prefect_variable,
        opensearch_repo,
        s3_bucket,
        indices,
        aws_operations_role,
    )


@flow(name="INS OpenSearch backup", log_prints=True)
def ins_opensearch_backup_prefect(
    environment: environment_choices,  # type: ignore
    snapshot_name: str,
    s3_bucket: str,
    opensearch_repo: str,
    indices: List[str] = [],
):
    """Create an INS OpenSearch snapshot.

    The environment selects the correct AWS secret containing ``es_host``.
    The snapshot and operations roles remain deployment configuration rather
    than run-time operator inputs.

    Args:
        environment: INS environment whose OpenSearch endpoint will be backed up.
        snapshot_name: Unique name to assign to the OpenSearch snapshot.
        s3_bucket: S3 bucket used by the snapshot repository.
        opensearch_repo: Registered snapshot repository name.
        indices: Index names to back up. Leave the array empty to back up all
            non-hidden indices.
    """
    run_opensearch_backup(
        snapshot_name,
        ins_dropdown_config[environment][INS_SECRET],
        ins_prefect_config["opensearch_snapshot_role_prefect_variable"],
        opensearch_repo,
        s3_bucket,
        indices,
        ins_prefect_config["opensearch_operations_role"],
    )

if __name__ == "__main__":
    # create your first deployment
   opensearch_backup_prefect.serve(name="opensearch_backup")
