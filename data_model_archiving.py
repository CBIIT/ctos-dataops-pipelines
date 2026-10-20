import subprocess
import os
import glob
import tempfile
from bento.common.s3 import upload_log_file
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME

def uplaod_s3(s3_bucket, s3_folder, log, model_file_list):
    # Upload to s3 bucket
    dest = f"s3://{s3_bucket}/{s3_folder}"
    try:
        for model_file in model_file_list:
            upload_log_file(dest, model_file)
            log.info(f'Uploading neo4j summary file {os.path.basename(model_file)} succeeded!')
    except Exception as e:
        log.error(e)

def data_model_archiving(model_repo, model_version, s3_bucket, s3_folder):
    if LOG_PREFIX not in os.environ:
        os.environ[LOG_PREFIX] = 'Data_Model_Archiving'
    os.environ[APP_NAME] = 'Data_Model_Archiving'

    log = get_logger('Data Model Archiving')
    log.info(f"Start cloning the data model repository from {model_repo}")
    # Fetch the selected remote ref for each run instead of reusing a checkout.
    with tempfile.TemporaryDirectory(prefix="ins-model-backup-") as model_dir:
        model_folder = os.path.join(model_dir, "model")
        subprocess.run(
            ['git', 'clone', '--depth', '1', '--single-branch', '--branch',
             model_version, '--', model_repo, model_folder],
            check=True,
        )
        log.info(f"Finished cloning the data model repository from {model_repo}")
        model_yaml_files = glob.glob(f'{model_folder}/**/*model*.yaml')
        model_yml_files = glob.glob(f'{model_folder}/**/*model*.yml')
        model_file_list = model_yaml_files + model_yml_files
        if not model_file_list:
            raise ValueError(f"No model YAML schemas found for {model_version!r} in {model_repo}")
        uplaod_s3(s3_bucket, s3_folder, log, model_file_list)
