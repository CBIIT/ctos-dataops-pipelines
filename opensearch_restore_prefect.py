from prefect import flow
from typing import Literal
import yaml
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from bento.common.secret_manager import get_secret
import os
import prefect.variables as Variable
from opensearch_restore import opensearch_restore


SECRET = "secret"
ES_HOST = "es_host"
PROJECT_NAME  = "icdc"
REGION = "us-east-1"
ENVIRONMENT = "env"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'OpenSearch Restore'
    os.environ[APP_NAME] = 'OpenSearch Restore'

config_file = "config/prefect_drop_down_config_icdc.yaml"
with open(config_file, 'r') as file:
    config = yaml.safe_load(file)
environment_choices = Literal[tuple(list(config.keys()))]

@flow(name="OpenSearch Restore", log_prints=True)
def opensearch_restore_prefect(
    environment: environment_choices, # type: ignore
    snapshot_name,
    indices,
    s3_bucket
):
    log = get_logger('OpenSearch Restore')
    secret_name = Variable.get(config[environment][SECRET])
    secret = get_secret(secret_name)
    role_arn = Variable.get("icdc_role_arn")
    os_role_arn = Variable.get("icdc_os_role_arn")
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
    opensearch_restore(argList)

if __name__ == "__main__":
   opensearch_restore_prefect.serve(name="opensearch_restore")