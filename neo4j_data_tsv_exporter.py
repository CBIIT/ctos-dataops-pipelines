
import csv
import yaml
from neo4j import GraphDatabase

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

def write_to_tsv(output_key, node, results, query_parent_dict):
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
    print(f"Data has been written to {output_key}")

def create_query(config, node, schema):
    query = f"MATCH (n:{node})"
    if WHERE in config.keys():
        if isinstance(config[WHERE], dict):
            for w in config[WHERE]:
                prop_node = w.split(".")[0]
                prop = w.split(".")[1]
                pv = config[WHERE][w]
                if node == prop_node:
                    if WHERE not in query:
                        query = query + f" WHERE n.{prop} IN {str(pv)}"
                    else:
                        query = query + f" AND n.{prop} in {str(pv)}"
    query_parent_dict = {}
    parent_list = check_parents(node, schema)
    if len(parent_list) > 0:
        query_parent_count = 0
        for parent in parent_list:
            query_parent_count += 1
            query_parent_value = "p" + str(query_parent_count)
            query = query + f" OPTIONAL MATCH (n)-->({query_parent_value}:{parent})"
            query_parent_dict[query_parent_value] = {parent: check_parents_id(parent, schema)}
    query = query + " Return n"
    if len(query_parent_dict.keys())>0:
        for qpv in query_parent_dict.keys():
            query = query + f",{qpv}"
    return query, query_parent_dict

def get_schema(config):
    org_schema = {}
    for aFile in config[SCHEMA]:
        print('Reading schema file: {} ...'.format(aFile))
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
    with open("config/neo4j_data_tsv_exporter_config_example.yaml") as f:
        config = yaml.safe_load(f)
    schema = get_schema(config)

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
            output_key = node + ".tsv"
            write_to_tsv(output_key, node, results, query_parent_dict)

if __name__ == '__main__':
    main()