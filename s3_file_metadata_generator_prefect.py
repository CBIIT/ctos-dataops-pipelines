from prefect import flow, task
from s3_file_metadata_generator import get_all_files_and_metadata
from bento.common.utils import get_logger
import sys

@flow(name="S3 File Metadata Generator", log_prints=True)
def s3_file_metadata_generator_prefect(
    bucket_name,
    s3_subfolder,
    tsv_filename
):
    log = get_logger('S3 File Metadata Generator')
    running_result = get_all_files_and_metadata(bucket_name, s3_subfolder, tsv_filename, log)

    if running_result:
        log.info("S3 File Metadata Generator finished successfully")
    else:
        log.error("S3 File Metadata Generator failed")
        sys.exit(1)

if __name__ == "__main__":
    # create your first deployment
   s3_file_metadata_generator_prefect.serve(name="s3_file_metadata_generator")