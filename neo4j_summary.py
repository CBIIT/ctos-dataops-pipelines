from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME, get_time_stamp
from bento.common.s3 import upload_log_file
from neo4j import GraphDatabase
import os
import json
import time

def uplaod_s3(s3_bucket, s3_folder, upload_file_key, log):
    dest = f"s3://{s3_bucket}/{s3_folder}"
    try:
        upload_log_file(dest, upload_file_key)
        log.info(f'Uploading neo4j summary file {os.path.basename(upload_file_key)} succeeded!')
    except Exception as e:
        log.error(e)

def neo4j_summary(neo4j_ip, neo4j_user, neo4j_password, summary_file_key, s3_bucket, s3_folder):
    TOTAL_NODES = "total_nodes"
    TOTAL_RELATIONSHIP = "total_relationships"
    NODE_TYPE = "node_type"
    COUNT = "count"
    NODE_COUNTS = "node_counts"
    RELATIONSHIP_COUNTS = "relationship_counts"
    RELATIONSHIP_TYPE = "relationship_type"
    TMP = "tmp"
    if LOG_PREFIX not in os.environ:
        os.environ[LOG_PREFIX] = 'Neo4j_Summary'
    os.environ[APP_NAME] = 'Neo4j_Summary'

    log = get_logger('Neo4j Summary Generator')
    neo4j_dict = {}
    neo4j_uri = "bolt://" + neo4j_ip + ":7687"
    driver = GraphDatabase.driver(
                        neo4j_uri,
                        auth=(neo4j_user, neo4j_password),
                        encrypted=False
                    )
    with driver.session() as session:
        log.info("Start generating neo4j summary")
        session = driver.session()
        max_retry = 3
        for i in range (0, max_retry):
            try:
                test_statement = "Match () Return 1 Limit 1"
                session.run(test_statement)
                break
            except Exception as e:
                log.error(e)
                time.sleep(10)
                #sys.exit(1)
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
        timestamp = get_time_stamp()
        new_summary_file_key = os.path.join(TMP, summary_file_key.replace(os.path.splitext(summary_file_key)[1], "_" + timestamp + ".json"))
        with open(new_summary_file_key, "w") as json_file:
            json.dump(neo4j_dict, json_file, indent=4)
        uplaod_s3(s3_bucket, s3_folder, new_summary_file_key, log)
        return neo4j_dict

