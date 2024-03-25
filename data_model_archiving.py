import subprocess
import argparse
import os
from bento.common.s3 import upload_log_file
from neo4j_summary import process_arguments
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
import glob

S3_BUCKET = "s3_bucket"
S3_FOLDER = "s3_prefix"
DATA_MODEL_REPO = "data_model_repo_url"
DATA_MODEL_VERSION = "data_model_version"
MODEL_FOLDER = "model"
argument_list = [DATA_MODEL_REPO, DATA_MODEL_REPO, S3_FOLDER, S3_BUCKET]
if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Data_Model_Archiving'
os.environ[APP_NAME] = 'Data_Model_Archiving'

def parse_arguments():
    parser = argparse.ArgumentParser(description='Data model archiving')
    parser.add_argument('config_file', help='Confguration file', nargs='?', default=None)
    parser.add_argument('--data-model-repo-url', help='The data model repository GitHub URL')
    parser.add_argument('--data-model-version', help='The data model repository GitHub URL')
    parser.add_argument('--s3-bucket', help='The upload s3 file bucket')
    parser.add_argument('--s3-prefix', help='The upload s3 file folder')
    return parser.parse_args()

def uplaod_s3(config_data, log, model_file_list):
    # Upload to s3 bucket
    if S3_BUCKET in config_data.keys():
        if S3_BUCKET is not None:
            if S3_FOLDER in config_data.keys():
                if S3_FOLDER is not None:
                    dest = f"s3://{config_data[S3_BUCKET]}/{config_data[S3_FOLDER]}"
                else:
                    dest = f"s3://{config_data[S3_BUCKET]}"
            else:
                dest = f"s3://{config_data[S3_BUCKET]}"
            try:
                for model_file in model_file_list:
                    upload_log_file(dest, model_file)
                    log.info(f'Uploading neo4j summary file {os.path.basename(model_file)} succeeded!')
            except Exception as e:
                log.error(e)

def main(args):
    log = get_logger('Data Model Archiving')
    config = process_arguments(args, log, argument_list)
    config_data = config.data
    #
    log.info(f"Start cloning the data model repository from {config_data[DATA_MODEL_REPO]}")
    subprocess.run(['git', 'clone', config_data[DATA_MODEL_REPO]])
    model_folder = os.path.splitext(os.path.basename(config_data[DATA_MODEL_REPO]))[0]
    subprocess.run(['git', '-C', model_folder, 'checkout', config_data[DATA_MODEL_VERSION]])
    log.info(f"Finished cloning the data model repository from {config_data[DATA_MODEL_REPO]}")
    model_yaml_files = glob.glob(f'{model_folder}/**/*model*.yaml')
    model_yml_files = glob.glob(f'{model_folder}/**/*model*.yml')
    model_file_list = model_yaml_files + model_yml_files
    if len(model_file_list) > 0:
        uplaod_s3(config_data, log, model_file_list)


if __name__ == '__main__':
    main(parse_arguments())