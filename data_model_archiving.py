import subprocess
import os
import glob
from bento.common.s3 import upload_log_file
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Data_Model_Archiving'
os.environ[APP_NAME] = 'Data_Model_Archiving'

def uplaod_s3(s3_bucket, s3_folder, log, model_file_list):
    # Upload to s3 bucket
    dest = f"s3://{s3_bucket}/{s3_folder}"
    try:
        for model_file in model_file_list:
            upload_log_file(dest, model_file)
            log.info(f'Uploading neo4j summary file {os.path.basename(model_file)} succeeded!')
    except Exception as e:
        log.error(e)

def model_archiving(model_repo, model_version, s3_bucket, s3_folder):
    log = get_logger('Data Model Archiving')
    log.info(f"Start cloning the data model repository from {model_repo}")
    subprocess.run(['git', 'clone', model_repo])
    model_folder = os.path.splitext(os.path.basename(model_repo))[0]
    subprocess.run(['git', '-C', model_folder, 'checkout', model_version])
    log.info(f"Finished cloning the data model repository from {model_repo}")
    model_yaml_files = glob.glob(f'{model_folder}/**/*model*.yaml')
    model_yml_files = glob.glob(f'{model_folder}/**/*model*.yml')
    model_file_list = model_yaml_files + model_yml_files
    if len(model_file_list) > 0:
        uplaod_s3( s3_bucket, s3_folder, log, model_file_list)