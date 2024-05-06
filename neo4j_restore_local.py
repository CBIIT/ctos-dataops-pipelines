from neo4j_summary_local import process_arguments
import argparse
import os
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from neo4j_restore import neo4j_restore
NEO4J_IP = "neo4j_ip"
NEO4J_USER = "neo4j_user"
NEO4J_KEY = "neo4j_key"
S3_FILE_KEY = "s3_file_key"
S3_BUCKET = "s3_bucket"
argument_list = [NEO4J_IP, NEO4J_USER, NEO4J_KEY, S3_FILE_KEY, S3_BUCKET]
if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Neo4j_Dump_Generator'
os.environ[APP_NAME] = 'Neo4j_Dump_Generator'

def parse_arguments():
    parser = argparse.ArgumentParser(description='Generate neo4j database summary')
    parser.add_argument('config_file', help='Confguration file', nargs='?', default=None)
    parser.add_argument('--neo4j-ip', help='The neo4j uri address')
    parser.add_argument('--neo4j-user', help='The neo4j user')
    parser.add_argument('--neo4j-key', help='The neo4j private key')
    parser.add_argument('--s3-bucket', help='The s3 file bucket that has the database dump file')
    parser.add_argument('--s3-file-key', help='The s3 file location of the database dump file')
    return parser.parse_args()

def main(args):
    log = get_logger('Neo4j Dump Generator')
    config = process_arguments(args, log, argument_list)
    config_data = config.data
    neo4j_restore(config_data[NEO4J_IP], config_data[NEO4J_USER], config_data[NEO4J_KEY], config_data[S3_BUCKET], config_data[S3_FILE_KEY])

if __name__ == '__main__':
    main(parse_arguments())