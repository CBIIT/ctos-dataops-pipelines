
import sys
import argparse
import os
import yaml
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from neo4j_summary import neo4j_summary

S3_BUCKET = "s3_bucket"
S3_FOLDER = "s3_folder"
NEO4J_URI = "neo4j_uri"
NEO4J_USER = "neo4j_user"
NEO4J_PASSWORD = "neo4j_password"
NEO4J_SUMMARY_FILE_NAME = "neo4j_summary_file_name"
argument_list = [NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_SUMMARY_FILE_NAME]

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Neo4j_Summary'
os.environ[APP_NAME] = 'Neo4j_Summary'


class Neo4jConfig:
    def __init__(self, config_file, args, config_file_arg='config_file'):
        self.log = get_logger('Neo4j Config')
        self.data = {}

        self.config_file_arg = config_file_arg
        if config_file:
            with open(config_file) as c_file:
                self.data = yaml.safe_load(c_file)['Config']
                if self.data is None:
                    self.data = {}
        self._override(args)

    def _override(self, args):
        for key, value in vars(args).items():
            # Ignore config file argument
            if key == self.config_file_arg:
                continue
            if isinstance(value, bool):
                if value:
                    self.data[key] = value

            elif value is not None:
                self.data[key] = value

def check_argument(config, argument_list, log):
    for argument in argument_list:
        if argument not in config.data.keys():
            log.error(f'The argument {argument} is missing!')
            sys.exit(1)
        else:
            if config.data[argument] is None:
                log.error(f'The argument {argument} is missing!')
                sys.exit(1)

def process_arguments(args, log, argument_list):
    config_file = None
    if args.config_file:
        config_file = args.config_file
    config = Neo4jConfig(config_file, args)
    #argument_list = [MANIFEST_FILE, FILE_NAME_COLUMN, FILE_SIZE_COLUMN, FILE_MD5_COLUMN]
    check_argument(config, argument_list, log)
    return config

def parse_arguments():
    parser = argparse.ArgumentParser(description='Generate neo4j database summary')
    parser.add_argument('config_file', help='Confguration file', nargs='?', default=None)
    parser.add_argument('--neo4j-uri', help='The neo4j uri')
    parser.add_argument('--neo4j-user', help='The neo4j user')
    parser.add_argument('--neo4j-password', help='The neo4j password')
    parser.add_argument('--s3-bucket', help='The upload s3 file bucket')
    parser.add_argument('--s3-folder', help='The upload s3 file folder')
    parser.add_argument('--neo4j-summary-file-name', help='The neo4j sumary file name')
    return parser.parse_args()

def main(args):
    log = get_logger('Neo4j Summary Generator')
    config = process_arguments(args, log, argument_list)
    config_data = config.data
    neo4j_summary(config_data[NEO4J_URI], config_data[NEO4J_USER], config_data[NEO4J_PASSWORD], config_data[NEO4J_SUMMARY_FILE_NAME], config_data[S3_BUCKET], config_data[S3_FOLDER])

if __name__ == '__main__':
    main(parse_arguments())