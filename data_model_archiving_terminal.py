import argparse
import os
from neo4j_summary import process_arguments
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from data_model_archiving import model_archiving

S3_BUCKET = "s3_bucket"
S3_FOLDER = "s3_prefix"
DATA_MODEL_REPO = "data_model_repo_url"
DATA_MODEL_VERSION = "data_model_version"
if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Data_Model_Archiving'
os.environ[APP_NAME] = 'Data_Model_Archiving'

argument_list = [DATA_MODEL_REPO, DATA_MODEL_REPO, S3_FOLDER, S3_BUCKET]
def parse_arguments():
    parser = argparse.ArgumentParser(description='Data model archiving')
    parser.add_argument('config_file', help='Confguration file', nargs='?', default=None)
    parser.add_argument('--data-model-repo-url', help='The data model repository GitHub URL')
    parser.add_argument('--data-model-version', help='The data model repository GitHub URL')
    parser.add_argument('--s3-bucket', help='The upload s3 file bucket')
    parser.add_argument('--s3-prefix', help='The upload s3 file folder')
    return parser.parse_args()

def main(args):
    log = get_logger('Data Model Archiving')
    config = process_arguments(args, log, argument_list)
    config_data = config.data
    #
    model_archiving(config_data[DATA_MODEL_REPO], config_data[DATA_MODEL_VERSION], config_data[S3_BUCKET], config_data[S3_FOLDER])


if __name__ == '__main__':
    main(parse_arguments())