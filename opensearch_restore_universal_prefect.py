from prefect import flow
from typing import List, Literal
import yaml
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from bento.common.secret_manager import get_secret
import os
import prefect.variables as Variable
from opensearch_restore import opensearch_restore
from opensearch_backup_prefect import get_aws_account_id
from opensearch_backup_universal_prefect import resolve_role, resolve_operations_role
import boto3

SUMARY_SECRET = "memgraph_summary_secret"
INS_SECRET = "neo4j_summary_secret"
ES_HOST = "es_host"
REGION = "us-east-1"
ENVIRONMENT = "env"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'OpenSearch Restore'
    os.environ[APP_NAME] = 'OpenSearch Restore'

INS_DROPDOWN_CONFIG_FILE = "config/prefect_drop_down_config.yaml"
INS_PREFECT_CONFIG_FILE = "config/ins-prefect.yaml"
with open(INS_DROPDOWN_CONFIG_FILE, 'r') as file:
    ins_dropdown_config = yaml.safe_load(file)
with open(INS_PREFECT_CONFIG_FILE, 'r') as file:
    ins_prefect_config = yaml.safe_load(file) or {}

environment_choices = Literal[tuple(ins_dropdown_config.keys())]


def run_opensearch_restore(
    snapshot_name,
    secret_name_prefect_variable,
    aws_role_prefect_variable,
    opensearch_repo,
    indices,
    s3_bucket,
    aws_operations_role,
):
    log = get_logger('OpenSearch Restore')
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
    opensearch_restore(argList)

@flow(name="OpenSearch restore", log_prints=True)
def opensearch_restore_prefect(
    snapshot_name: str,
    secret_name_prefect_variable: str,
    aws_role_prefect_variable: str,
    opensearch_repo: str,
    s3_bucket: str,
    indices: str = "",
    aws_operations_role: str = "",
):
    run_opensearch_restore(
        snapshot_name,
        secret_name_prefect_variable,
        aws_role_prefect_variable,
        opensearch_repo,
        indices,
        s3_bucket,
        aws_operations_role,
    )


@flow(name="OpenSearch restore", log_prints=True)
def ins_opensearch_restore_prefect(
    environment: environment_choices,  # type: ignore
    snapshot_name: str,
    s3_bucket: str,
    opensearch_repo: str,
    indices: List[str] = [],
):
    """Restore an INS OpenSearch snapshot.

    The environment selects the correct AWS secret containing ``es_host``.
    Leaving the indices array empty restores every non-hidden index in the snapshot.

    Args:
        environment: INS environment whose OpenSearch endpoint will be restored.
        snapshot_name: Exact name of the snapshot to restore.
        s3_bucket: S3 bucket containing the snapshot.
        opensearch_repo: Snapshot repository name.
        indices: Index names to restore. Leave the array empty to restore all
            non-hidden indices.
    """
    run_opensearch_restore(
        snapshot_name,
        ins_dropdown_config[environment][INS_SECRET],
        ins_prefect_config["opensearch_snapshot_role_prefect_variable"],
        opensearch_repo,
        indices,
        s3_bucket,
        ins_prefect_config["opensearch_operations_role"],
    )

if __name__ == "__main__":
    # create your first deployment
   opensearch_restore_prefect.serve(name="opensearch_restore")
