
import csv
import yaml
from bento.common.utils import get_time_stamp, get_logger, LOG_PREFIX, APP_NAME
from neo4j import GraphDatabase
import os
from collections import defaultdict

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
SRC = "Src"
DST = "Dst"
MUL = "Mul"
FILTER_RELATED_NODES = "filter_related_nodes"
if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Neo4j_TSV_Export'
    os.environ[APP_NAME] = 'Neo4j_TSV_Export'

def find_all_paths(paths, start, end):
    graph = defaultdict(list)
    for path in paths:
        graph[path[SRC]].append(path[DST])
        graph[path[DST]].append(path[SRC])# Add reverse connection for the graph
    all_paths = []
    def dfs(current, path, visited):
        if current == end:
            all_paths.append(path)
            return
        visited.add(current)
        for neighbor in graph[current]:
            if neighbor not in visited:  # Prevent cycles
                dfs(neighbor, path + [neighbor], visited)
        visited.remove(current)
    dfs(start, [start], set())

    return all_paths

def write_to_tsv(output_key, node, results, query_parent_dict, log):
    if results.peek():
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

def collect_path(schema):
    paths = []
    for real in schema[RELATIONSHIPS].keys():
        #paths = paths + schema[RELATIONSHIPS][real][ENDS].pop(MUL)
        for end in schema[RELATIONSHIPS][real][ENDS]:
            end.pop(MUL, None)
            paths.append(end)
    return paths

def find_path_direction(separate_path, paths):
    for path in paths:
        path_list = list(path.values())
        if set(path_list) == set(separate_path) and len(path_list) == len(separate_path):
            return path
    return None

def query_match_update(paths, all_paths, node):
    for path in all_paths:
        query_match_list = []
        diff_direction_query = []
        diff_direction_exist = False
        pass_node = {}
        query_update = ""
        separate_path_list = [path[i:i+2] for i in range(len(path)-1)]
        #query_match = query_match + " MATCH "
        node_query = f"({node})"
        for separate_path in separate_path_list:
            position = "right"
            query_direction = ""
            path_diretion = find_path_direction(separate_path, paths)
            if path_diretion is not None:
                if not pass_node:
                    query_direction = f"({path_diretion[SRC]})-->({path_diretion[DST]})"
                elif path_diretion[SRC] == pass_node[DST]:
                    query_direction = f"-->({path_diretion[DST]})"
                elif path_diretion[DST] == pass_node[SRC]:
                    query_direction = f"({path_diretion[SRC]})-->"
                    position = "left"
                elif path_diretion[DST] == pass_node[DST]:
                    query_direction = f"<--({path_diretion[SRC]})"
                    diff_direction_exist = True
                elif path_diretion[SRC] == pass_node[SRC]:
                    query_direction = f"({path_diretion[DST]})<--"
                    diff_direction_exist = True
                    position = "left"
            if position == "right":
                query_update = query_update + query_direction
            else:
                query_update = query_direction + query_update
            query_update = query_update.replace(node_query, "(n)")
            pass_node[SRC] = path_diretion[SRC]
            pass_node[DST] = path_diretion[DST]
        query_match_list.append(query_update)
        if diff_direction_exist:
            diff_direction_query.append(query_update)
    # The query with different direction inside will be dropped if there is query with same direction exists.
    if len(diff_direction_query) == len(query_match_list):
        return query_match_list
    else:
        updated_query_match_list = [q for q in query_match_list if q not in diff_direction_query]
        return updated_query_match_list


def create_query(config, node, schema, log):
    #query = f"MATCH (n:{node})"
    parent_list = check_parents(node, schema)
    query_match = f"MATCH (n:{node})"
    secondary_query_match_list = []
    query_where = ""
    query_optional = ""
    query_return = " Return distinct(n)"
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
                filter_related_nodes = config[WHERE][w].get(FILTER_RELATED_NODES)
                if filter_related_nodes is None:
                    filter_related_nodes = True
                if node == prop_node:
                    if "WHERE" not in query_where:
                        query_where = query_where + f" WHERE n.{prop} IN {str(pv)}"
                    else:
                        query_where = query_where + f" AND n.{prop} in {str(pv)}"
                elif filter_related_nodes:
                    paths = collect_path(schema)
                    all_paths = find_all_paths(paths, node, prop_node)
                    if all_paths:
                        secondary_query_match_list = query_match_update(paths, all_paths, node)
                        if "WHERE" not in query_where:
                            query_where = query_where + f" WHERE {prop_node}.{prop} IN {str(pv)}"
                        else:
                            query_where = query_where + f" AND {prop_node}.{prop} IN {str(pv)}"
                    else:
                        log.error(f"can not find relationship between {prop_node} and {node}")
    if secondary_query_match_list:
        query_list = []
        for secondary_query_match in secondary_query_match_list:
            query_list.append(query_match + " MATCH " + secondary_query_match + query_where + query_optional + query_return)
        query = " UNION ".join(query_list)
    else:
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
    timestamp = get_time_stamp()
    node_list = config.get("nodes")
    if node_list is None:
        node_list = list(schema[NODES].keys())
    for node in node_list:
        query, query_parent_dict = create_query(config, node, schema, log)
        with driver.session() as session:
            session = driver.session()
            results = session.run(query)
            folder_path = os.path.join(TMP, "tsv_exporter" + "-" + timestamp)
            if not os.path.exists(folder_path):
                os.mkdir(folder_path)
            output_key = os.path.join(folder_path, node + ".tsv") 
            write_to_tsv(output_key, node, results, query_parent_dict, log)

if __name__ == '__main__':
    main()