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
OPERATIONS_ROLE_VARIABLE = "aws_operations_role"

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
  # Empty means "use task credentials"; only look up the Prefect variable when no explicit value was passed.
  if not aws_operations_role:
    try:
      aws_operations_role = Variable.get(OPERATIONS_ROLE_VARIABLE) or ""
    except Exception as e:
      log.info(f"No {OPERATIONS_ROLE_VARIABLE} Prefect variable found: {e}")
      aws_operations_role = ""
  if not aws_operations_role:
    return ""
  return resolve_role(aws_operations_role, log)

@flow(name="OpenSearch backup", log_prints=True)
def opensearch_backup_prefect(
    snapshot_name,
    secret_name_prefect_variable,
    aws_role_prefect_variable,
    opensearch_repo,
    s3_bucket,
    indices,
    aws_operations_role=""
):
    log = get_logger('OpenSearch Backup')
    opensearch_secret = Variable.get(secret_name_prefect_variable)
    secret = get_secret(opensearch_secret)
    snapshot_role = resolve_role(aws_role_prefect_variable, log)
    role_arn = snapshot_role
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

if __name__ == "__main__":
    # create your first deployment
   opensearch_backup_prefect.serve(name="opensearch_backup")