
import csv
import yaml
from bento.common.utils import get_time_stamp, get_logger, LOG_PREFIX, APP_NAME
from neo4j import GraphDatabase
import os

SCHEMA = "schema"
RELATIONSHIPS = "Relationships"
TYPE = "type"
WHERE = "where"
SRC = "Src"
DST = "Dst"
ENDS = "Ends"
NODES = "Nodes"
PROPS = "Props"
PROP_DEFINITIONS = "PropDefinitions"
KEY = "Key"
TMP = "tmp"
if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Neo4j_TSV_Export'
    os.environ[APP_NAME] = 'Neo4j_TSV_Export'


def write_to_tsv(output_key, node, results, query_parent_dict, log):
    with open(output_key, "w", newline="") as csvfile:
        fieldnames = set()
        row_list = []
        for record in results:
            result_list = list(record.keys())
            for r in result_list:
                if r == "n":
                    row = {key: value for key, value in record[r].items() if key != "created"}
                else:
                    column_name = ""
                    for parent, parent_id in query_parent_dict[r].items():
                        column_name = parent + "." + parent_id
                        if record[r] is not None:
                            row[column_name] = record[r][parent_id]
                        else:
                            row[column_name] = None
            row[TYPE] = node
            row_list.append(row)
            fieldnames.update(row.keys())

        fieldname_list = list(fieldnames)
        writer = csv.DictWriter(csvfile, fieldnames=fieldname_list, delimiter='\t')
        writer.writeheader()
        for r in row_list:
            writer.writerow(r)
    log.info(f"Data has been written to {output_key}")

def create_query(config, node, schema):
    #query = f"MATCH (n:{node})"
    parent_list = check_parents(node, schema)
    query_match = f"MATCH (n:{node})"
    query_where = ""
    query_optional = ""
    query_return = " Return n"
    query_parent_dict = {}
    if len(parent_list) > 0:
        query_parent_count = 0
        for parent in parent_list:
            query_parent_count += 1
            query_parent_value = "p" + str(query_parent_count)
            query_optional = query_optional + f" OPTIONAL MATCH (n)-->({query_parent_value}:{parent})"
            query_parent_dict[query_parent_value] = {parent: check_parents_id(parent, schema)}
    if len(query_parent_dict.keys())>0:
        for qpv in query_parent_dict.keys():
            query_return = query_return + f",{qpv}"
    if WHERE in config.keys():
        if isinstance(config[WHERE], dict):
            for w in config[WHERE]:
                prop_node = w.split(".")[0]
                prop = w.split(".")[1]
                pv = config[WHERE][w].get("values")
                where_connected = config[WHERE][w].get("where_connected")
                if node == prop_node:
                    if "WHERE" not in query_where:
                        query_where = query_where + f" WHERE n.{prop} IN {str(pv)}"
                    else:
                        query_where = query_where + f" AND n.{prop} in {str(pv)}"
                elif prop_node in parent_list and where_connected:
                    query_match = query_match + f" MATCH (n)-->({prop_node}:{prop_node})"
                    if "WHERE" not in query_where:
                        query_where = query_where + f" WHERE {prop_node}.{prop} IN {str(pv)}"
                    else:
                        query_where = query_where + f" AND {prop_node}.{prop} IN {str(pv)}"
    query = query_match + query_where + query_optional + query_return
    return query, query_parent_dict

def get_schema(config, log):
    org_schema = {}
    for aFile in config[SCHEMA]:
        log.info('Reading schema file: {} ...'.format(aFile))
        with open(aFile) as schema_file:
            schema = yaml.safe_load(schema_file)
            if schema:
                org_schema.update(schema)
    return org_schema

def check_parents(node, schema):
    parent_list = []
    for real_type in schema[RELATIONSHIPS]:
        for dest in schema[RELATIONSHIPS][real_type][ENDS]:
            if node == dest[SRC]:
                parent_list.append(dest[DST])
    return parent_list

def check_parents_id(node, schema):
    for prop in schema[NODES][node][PROPS]:
        if KEY in schema[PROP_DEFINITIONS][prop]:
            if schema[PROP_DEFINITIONS][prop][KEY]:
                return prop
    return None



def main():
    log = get_logger('Neo4j Data TSV Exporting')
    with open("config/neo4j_data_tsv_exporter_config_example.yaml") as f:
        config = yaml.safe_load(f)
    schema = get_schema(config, log)

    # Connect to the Neo4j database
    driver = GraphDatabase.driver(
            config["bolt_port"],
            auth=(config["neo4j_user"], config["neo4j_password"]),
            encrypted=False
        )
    for node in config["nodes"]:
        query, query_parent_dict = create_query(config, node, schema)
        with driver.session() as session:
            session = driver.session()
            results = session.run(query)
            timestamp = get_time_stamp()
            folder_path = os.path.join(TMP, "tsv_exporter" + "-" + timestamp)
            if not os.path.exists(folder_path):
                os.mkdir(folder_path)
            output_key = os.path.join(folder_path, node + ".tsv") 
            write_to_tsv(output_key, node, results, query_parent_dict, log)

if __name__ == '__main__':
    main()