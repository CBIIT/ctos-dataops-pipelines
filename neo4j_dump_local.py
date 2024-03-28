from neo4j_summary_local import process_arguments
import argparse
import os
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from neo4j_dump import neo4j_dump
NEO4J_URI = "neo4j_uri"
NEO4J_USER = "neo4j_user"
NEO4J_PASSWORD = "neo4j_password"
DUMP_FILE = "dump_file_name"
S3_FOLDER = "s3_folder"
S3_BUCKET = "s3_bucket"
argument_list = [NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DUMP_FILE, S3_FOLDER, S3_BUCKET]
if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Neo4j_Dump_Generator'
os.environ[APP_NAME] = 'Neo4j_Dump_Generator'

def parse_arguments():
    parser = argparse.ArgumentParser(description='Generate neo4j database summary')
    parser.add_argument('config_file', help='Confguration file', nargs='?', default=None)
    parser.add_argument('--neo4j-uri', help='The neo4j uri address')
    parser.add_argument('--neo4j-user', help='The neo4j user')
    parser.add_argument('--neo4j-password', help='The neo4j password')
    parser.add_argument('--s3-bucket', help='The upload s3 file bucket')
    parser.add_argument('--s3-folder', help='The upload s3 file folder')
    parser.add_argument('--dump-file-name', help='The neo4j dump file name')
    return parser.parse_args()

def main(args):
    log = get_logger('Neo4j Dump Generator')
    config = process_arguments(args, log, argument_list)
    config_data = config.data
    neo4j_dump(config_data[DUMP_FILE], config_data[NEO4J_URI], config_data[NEO4J_USER], config_data[NEO4J_PASSWORD], config_data[S3_BUCKET], config_data[S3_FOLDER])
    

if __name__ == '__main__':
    main(parse_arguments())