from prefect import flow
from memgraph_export import memgraph_export
import os
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Memgraph_Export'
    os.environ[APP_NAME] = 'Memgraph_Export'

@flow(name="memgraph_export", log_prints=True)
def memgraph_export_prefect(
    memgraph_host,
    memgraph_port,
    memgraph_username,
    memgraph_password,
    tmp_folder,
    s3_bucket,
    s3_prefix,
):
    log = get_logger('Memgraph Export')
    memgraph_export(memgraph_host, memgraph_port, memgraph_username, memgraph_password, tmp_folder, s3_bucket, s3_prefix, log)

if __name__ == "__main__":
    # create your first deployment
   memgraph_export_prefect.serve(name="memgraph_export")