from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
from bento.common.s3 import upload_log_file
from neo4j import GraphDatabase
import yaml
import os
import json
import sys
import argparse

S3_BUCKET = "s3_bucket"
S3_FOLDER = "s3_folder"
TOTAL_NODES = "total_nodes"
TOTAL_RELATIONSHIP = "total_relationships"
NEO4J_URI = "neo4j_uri"
NEO4j_USER = "neo4j_user"
NEO4J_PASSWORD = "neo4j_password"
TEMP_FOLDER = 'tmp'
NODE_TYPE = "node_type"
COUNT = "count"
NODE_COUNTS = "node_counts"
RELATIONSHIP_COUNTS = "relationship_counts"
RELATIONSHIP_TYPE = "relationship_type"
NEO4j_SUMMARY_FILE_NAME = "neo4j_summary_file_name"

argument_list = [NEO4J_URI, NEO4j_USER, NEO4J_PASSWORD, NEO4j_SUMMARY_FILE_NAME]

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Stream_File_Validator'
os.environ[APP_NAME] = 'Stream_File_Validator'

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
    parser = argparse.ArgumentParser(description='Validate files through streming files from the s3 bucket')
    parser.add_argument('config_file', help='Confguration file', nargs='?', default=None)
    parser.add_argument('--neo4j_uri', help='The neo4j uri')
    parser.add_argument('--neo4j_user', help='The neo4j user')
    parser.add_argument('--neo4j_password', help='The neo4j password')
    parser.add_argument('--upload_s3_url', help='The upload s3 file location')
    return parser.parse_args()

def uplaod_s3(config_data, log):
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
                print(dest)
                upload_log_file(dest, config_data[NEO4j_SUMMARY_FILE_NAME])
                log.info(f'Uploading validation result zip file {os.path.basename(config_data[NEO4j_SUMMARY_FILE_NAME])} succeeded!')
            except Exception as e:
                log.error(e)

def main(args):
    log = get_logger('Neo4j Summary Generator')
    config = process_arguments(args, log, argument_list)
    neo4j_dict = {}
    config_data = config.data
    driver = GraphDatabase.driver(
                        config_data[NEO4J_URI],
                        auth=(config_data[NEO4j_USER], config_data[NEO4J_PASSWORD]),
                        encrypted=False
                    )
    with driver.session() as session:
        log.info("Start generating neo4j summary")
        session = driver.session()
        try:
            test_statement = "Match () Return 1 Limit 1"
            session.run(test_statement)
        except Exception as e:
            log.error(e)
            sys.exit(1)
        log.info("Connect to the neo4j database successfully")
        # Query the count for total nodes
        total_node_statement = f"MATCH (n) RETURN COUNT(n) as {TOTAL_NODES}"
        total_node_result = session.run(total_node_statement)
        for record in total_node_result:
            neo4j_dict[TOTAL_NODES] = record[TOTAL_NODES]
            log.info(f"Total nodes: {record[TOTAL_NODES]}")
        # Query the count for total relationships
        total_relationship_statement = f"MATCH ()-[r]->() RETURN COUNT(r) AS {TOTAL_RELATIONSHIP}"
        total_relationship_result = session.run(total_relationship_statement)
        for record in total_relationship_result:
            neo4j_dict[TOTAL_RELATIONSHIP] = record[TOTAL_RELATIONSHIP]
            log.info(f"Total relationships: {record[TOTAL_RELATIONSHIP]}")
        # Query the counts for each node type
        node_counts_statement = f"MATCH (n) RETURN labels(n) AS {NODE_TYPE}, COUNT(*) AS {COUNT}"
        node_counts_result = session.run(node_counts_statement)
        neo4j_dict[NODE_COUNTS] = {}
        node_counts_dict = {}
        for record in node_counts_result:
            node_counts_dict[record[NODE_TYPE][0]] = record[COUNT]
            log.info(f"Node {record[NODE_TYPE][0]}: {record[COUNT]}")
        neo4j_dict[NODE_COUNTS] = {k: node_counts_dict[k] for k in sorted(node_counts_dict)}
        # Query the counts for each relationship type
        relationship_counts_statement = f"MATCH ()-[r]->() RETURN TYPE(r) AS {RELATIONSHIP_TYPE}, COUNT(*) AS {COUNT}"
        relationship_counts_result = session.run(relationship_counts_statement)
        neo4j_dict[RELATIONSHIP_COUNTS] = {}
        relationship_counts_dict = {}
        for record in relationship_counts_result:
            relationship_counts_dict[record[RELATIONSHIP_TYPE]] = record[COUNT]
            log.info(f"Relationship {record[RELATIONSHIP_TYPE]}: {record[COUNT]}")    
        neo4j_dict[RELATIONSHIP_COUNTS] = {k: relationship_counts_dict[k] for k in sorted(relationship_counts_dict)}
        with open(config_data[NEO4j_SUMMARY_FILE_NAME], "w") as json_file:
            json.dump(neo4j_dict, json_file, indent=4)
        
        uplaod_s3(config_data, log)

if __name__ == '__main__':
    main(parse_arguments())