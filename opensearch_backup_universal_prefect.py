from prefect import flow
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from bento.common.secret_manager import get_secret
import os
import prefect.variables as Variable
from opensearch_backup import opensearch_backup

SUMARY_SECRET = "memgraph_summary_secret"
ES_HOST = "es_host"
REGION = "us-east-1"
ENVIRONMENT = "env"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'OpenSearch Backup'
    os.environ[APP_NAME] = 'OpenSearch Backup'

@flow(name="OpenSearch backup", log_prints=True)
def opensearch_backup_prefect(
    snapshot_name,
    secret_name_prefect_variable,
    opensearch_repo,
    s3_bucket,
    indices
):
    log = get_logger('OpenSearch Backup')
    opensearch_secret = Variable.get(secret_name_prefect_variable)
    secret = get_secret(opensearch_secret)
    argList = {
        'oshost': "https://" + secret[ES_HOST] + "/",
        'repo': opensearch_repo,
        's3bucket': s3_bucket,
        'snapshot': snapshot_name,
        'indices': indices,
        'region': REGION,
        'basepath': snapshot_name
    }
    opensearch_backup(argList)

if __name__ == "__main__":
    # create your first deployment
   opensearch_backup_prefect.serve(name="opensearch_backup")