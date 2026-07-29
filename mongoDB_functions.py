# script to export the given collection from MongoDB to a JSON file

import pymongo
import json
import os
from datetime import datetime
import pandas as pd
from bento.common.s3 import upload_log_file, S3Bucket
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
import yaml
import copy
import ijson



NODE_TYPE = "nodeType"
PARENTS = "parents"
PROPS = "props"
DATA_COMMONS = "dataCommons"
PARENT_TYPE = "parentType"
PARENT_ID_PROP_NAME = "parentIDPropName"
PARENT_ID_VALUE = "parentIDValue"
HISTORY = "history"
SUBMISSION_ID = "submissionID"
SUBMISSION_INTENTION = "intention"
RELEASED_AT = "releasedAt"
PARENTS = "parents"
UPDATE_LOG = "updateLog"
UPDATE_REASON = "updateReason"
UPDATE_TIMESTAMP = "updateTimestamp"
NODE_ID = "nodeID"
UPDATED_AT = "updatedAt"
CURRENT_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
INTENTION = "intention"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'MongoDB_Update'
    os.environ[APP_NAME] = 'MongoDB_Update'

log = get_logger('MongoDB_Update')

def stream_read_json(file_path):
    with open(file_path, "r") as f:
        data = []
        records = ijson.items(f, "item")
        for record in records:
            data.append(record)
    return data

def split_dump_json(json_list, output_file, chunk_size=5000):
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('[')
        first_chunk = True
        chunk = []
        for item in json_list:
            chunk.append(item)
            if len(chunk) >= chunk_size:
                if not first_chunk:
                    f.write(',')
                # Serialize the chunk as a JSON array, then write without outer brackets
                f.write(json.dumps(chunk, ensure_ascii=False, default=str)[1:-1])
                first_chunk = False
                chunk.clear()
                #log.info(f"Dumped {len(chunk)} items to {output_file}")
        if chunk:
            if not first_chunk:
                f.write(',')
            f.write(json.dumps(chunk, ensure_ascii=False, default=str)[1:-1])
            #log.info(f"Dumped {len(chunk)} items to {output_file}")
        f.write(']')
        log.info(f"Dumped {len(json_list)} items to {output_file}")

def export_collection(client, db_name, collection_name, exported_file, batch_size=5000):
    db = client[db_name]
    collection = db[collection_name]
    # Stream docs to file so the full collection is never held in memory
    cursor = collection.find().batch_size(batch_size)
    with open(exported_file, "w") as f:
        f.write("[\n")
        first = True
        count = 0
        for doc in cursor:
            if not first:
                f.write(",\n")
            # default=str handles ObjectId and other BSON types
            json.dump(doc, f, default=str)
            first = False
            count += 1
            if count % batch_size == 0:
                log.info(f"Exported {count} documents...")
        f.write("\n]")
    log.info(f"Exported {count} documents to {exported_file}")

def upload_s3(s3_bucket, s3_prefix, file_key, log):
    
    dest = os.path.join(f"s3://{s3_bucket}", s3_prefix)
    log.info(f'Start uploading the file to {dest}')
    upload_log_file(dest, file_key)
    log.info(f'Uploading the MongoDB export file {os.path.basename(file_key)} succeeded!')

def download_s3(s3_bucket, s3_file_key, log, file_key):
    log.info(f'Downloading file {os.path.basename(s3_file_key)} from {s3_bucket} to {file_key}')
    bucket = S3Bucket(s3_bucket)
    if not os.path.exists(os.path.dirname(file_key)):
        os.makedirs(os.path.dirname(file_key))
    bucket.download_file(s3_file_key, file_key)
    log.info(f'Downloading file {os.path.basename(s3_file_key)} succeeded!')

def add_initial_history_item(item):
    props = copy.deepcopy(item.get(PROPS))
    parents = copy.deepcopy(item.get(PARENTS))
    updated_at = copy.deepcopy(item.get(UPDATED_AT))
    item[HISTORY] = [{
        RELEASED_AT: updated_at,
        INTENTION: "New/Update",
        PROPS: props,
        PARENTS: parents,
    }]
    return item

def add_history_item(item, update_log, is_node_updated=False):
    if isinstance(item[HISTORY], list):
        if is_node_updated:
            item[HISTORY].append({
                RELEASED_AT: CURRENT_TIMESTAMP,
                PROPS: item.get(PROPS),
                PARENTS: item.get(PARENTS),
                NODE_ID: item.get(NODE_ID),
                UPDATE_LOG: update_log
            })
        else:
            item[HISTORY].append({
                RELEASED_AT: CURRENT_TIMESTAMP,
                PROPS: item.get(PROPS),
                PARENTS: item.get(PARENTS),
                UPDATE_LOG: update_log
            })
    else:
        log.error(f'History is not a list, it is {type(item[HISTORY])}')
        return item
    return item

def update_exported_collection(exported_file, updated_exported_file, update_reference_file, old_parent_id_field, new_parent_id_field, node, data_commons, log):
    try:
        
        #with open(exported_file, "r") as f:
        #    data = json.load(f)
        data = stream_read_json(exported_file)
        with open(update_reference_file, "r") as f:
            update_reference = pd.read_csv(f, sep="\t")
        counter = {"node_updated": 0, "children_updated": {}, "total_records_before_update": 0, "total_records_after_update": 0}
        update_dict = {}
        for index,row in update_reference.iterrows():
            update_dict[str(row[old_parent_id_field])] = str(row[new_parent_id_field])

        for item in data:
            
            if item[DATA_COMMONS] == data_commons:
                if item.get(NODE_TYPE) and item.get(NODE_TYPE) == node:
                    if item.get(PROPS).get(old_parent_id_field):
                        if update_dict.get(str(item[PROPS][old_parent_id_field])):
                            if not item.get(HISTORY):
                                item = add_initial_history_item(item)
                            item[PROPS][new_parent_id_field] = update_dict[str(item[PROPS][old_parent_id_field])]
                            item[NODE_ID] = update_dict[str(item[PROPS][old_parent_id_field])]
                            item[UPDATED_AT] = CURRENT_TIMESTAMP
                            update_log = {
                                UPDATE_REASON: "Add new ID property to the props",
                                UPDATE_TIMESTAMP: CURRENT_TIMESTAMP,
                            }
                            item = add_history_item(item, update_log, True)
                            counter["node_updated"] += 1
                            log.info(f"Updated node {{{old_parent_id_field} : {item[PROPS][old_parent_id_field]}}}  with new ID {{{new_parent_id_field} : {update_dict[str(item[PROPS][old_parent_id_field])]}}}.")
                else:
                    if item.get(PARENTS):
                        for index, parent in enumerate(item[PARENTS]):
                            if parent.get(PARENT_TYPE) and parent.get(PARENT_TYPE) == node:
                                if update_dict.get(str(parent[PARENT_ID_VALUE])):
                                    if not item.get(HISTORY):
                                        item = add_initial_history_item(item)
                                    item[PARENTS][index][PARENT_ID_PROP_NAME] = new_parent_id_field
                                    new_parent_id_value = update_dict[str(parent[PARENT_ID_VALUE])]
                                    item[PARENTS][index][PARENT_ID_VALUE] = new_parent_id_value
                                    item[UPDATED_AT] = CURRENT_TIMESTAMP
                                    update_log = {
                                        UPDATE_REASON: "Update parent ID property and value",
                                        UPDATE_TIMESTAMP: CURRENT_TIMESTAMP,
                                    }
                                    item = add_history_item(item, update_log, False)
                                    log.info(f"Updated child node {{type: {item[NODE_TYPE]}, node_id: {item[NODE_ID]}}} with new parent ID {{{new_parent_id_field} : {new_parent_id_value}}}.")
                                    if counter.get("children_updated").get(item[NODE_TYPE]):
                                        counter["children_updated"][item[NODE_TYPE]] += 1
                                    else:
                                        counter["children_updated"][item[NODE_TYPE]] = 1
        split_dump_json(data, updated_exported_file)
        log.info(f'Updated exported collection {updated_exported_file} successfully!')
        return updated_exported_file, counter
    except Exception as e:
        log.error(e)
        return None, None


def replace_many_in_batches(collection, data, batch_size=5000):
    try:
        operations = []
        count = 0
        for item in data: # Use a generator or iterator to avoid OOM
            # 1. Prepare your operation
            operation = pymongo.ReplaceOne(
                {"_id": item["_id"]}, 
                item,
                upsert=False
            )
            operations.append(operation)
            # 2. Flush when batch size is reached
            if len(operations) >= batch_size:
                collection.bulk_write(operations, ordered=False)
                count += len(operations)
                log.info(f"Processed {count} documents in the updated data")
                operations.clear()


        # 3. Flush the remaining operations
        if operations:
            collection.bulk_write(operations, ordered=False)
            count += len(operations)
            log.info(f"Processed {count} documents in the updated data")
            operations.clear()
        return True
    except Exception as e:
        log.error(e)
        return False
        
def export_counter(counter_file, counter):
    counter["total_updated_nodes"] = counter["node_updated"] + sum(counter["children_updated"].values())
    with open(counter_file, "w") as f:
        json.dump(counter, f, default=str, indent=2)

def import_collection(client, db_name, collection_name, updated_data_file, backup_file, counter, counter_file, log):
    try:
        #with open(backup_file, "r") as f:
        #    backup_data = json.load(f)
        backup_data = stream_read_json(backup_file)
        #with open(updated_data_file, "r") as f:
        #    data = json.load(f)
        data = stream_read_json(updated_data_file)
        # get total records count from the collection
        collection = client[db_name][collection_name]
        counter["total_records_before_update"] = collection.count_documents({})
        if len(data) != counter["total_records_before_update"]:
            log.error("Total records count mismatch between the updated data and the collection")
            return False
        log.info("Start replacing the collection with the new data")
        result = replace_many_in_batches(collection, data)
        counter["total_records_after_update"] = collection.count_documents({})
        export_counter(counter_file, counter)
        if counter["total_records_before_update"] != counter["total_records_after_update"]:
            log.error("Total records count mismatch before and after update")
            result = False
        else:
            log.info(f"Total records count {counter['total_records_after_update']} matched before and after update, and total updated nodes: {counter['total_updated_nodes']}")
        if result:
            return True
        else:
            log.error("Collection import failed")
            result = replace_many_in_batches(collection, backup_data)
            return False
    except Exception as e:
        # if failed, import the backup file
        result = replace_many_in_batches(collection, backup_data)
        return False

if __name__ == "__main__":
    # read the config file
    with open("config/mongodb_config_test.yaml", "r") as f:
        config = yaml.safe_load(f)
    mongo_url = config["mongo_url"]
    db_name = config["db_name"]
    collection_name = config["collection_name"]
    exported_file = config["exported_file"]
    update_reference_file = config["update_reference_file"]
    old_parent_id_field = config["old_parent_id_field"]
    new_parent_id_field = config["new_parent_id_field"]
    updated_exported_file = config["updated_exported_file"]
    node = config["node"]
    data_commons = config["data_commons"]
    client = pymongo.MongoClient(mongo_url)
    counter_file = config["counter_file"]
    export_collection(client, db_name, collection_name, exported_file)
    updated_data, counter = update_exported_collection(exported_file, updated_exported_file, update_reference_file, old_parent_id_field, new_parent_id_field, node, data_commons, log)
    if updated_data:
        result = import_collection(client, db_name, collection_name, updated_data, exported_file, counter, counter_file, log)
        if result:
            print("Collection imported successfully")
        else:
            print("Collection import failed")