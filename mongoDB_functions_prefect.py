from prefect import flow
from mongoDB_functions import import_collection, update_exported_collection, upload_s3, export_collection, download_s3
from bento.common.secret_manager import get_secret
import pymongo
import os
import shutil
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME


MONGO_DB_USER = "mongo_db_user"
MONGODB_PORT = "mongo_db_port"
MONGO_DB_HOST = "mongo_db_host"
MONGO_DB_PASSWORD = "mongo_db_password"
DOWNLOAD_FOLDER = "/tmp/download_folder"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'MongoDB_Update'
    os.environ[APP_NAME] = 'MongoDB_Update'

log = get_logger('MongoDB_Update')

@flow(name="MongoDB Database Update", log_prints=True)
def mongoDB_database_update_prefect(
    secret_name,
    db_name,
    collection_name,
    exported_file,
    updated_exported_file,
    counter_file,
    s3_update_reference_file,
    old_parent_id_field,
    new_parent_id_field,
    node,
    data_commons,
    s3_backup_bucket,
    s3_backup_folder
):
    secret = get_secret(secret_name)
    mongo_db_user = secret[MONGO_DB_USER]
    mongo_db_port = secret[MONGODB_PORT]
    mongo_db_host = secret[MONGO_DB_HOST]
    mongo_db_password = secret[MONGO_DB_PASSWORD]
    mongo_url = f"mongodb://{mongo_db_user}:{mongo_db_password}@{mongo_db_host}:{mongo_db_port}/"
    client = pymongo.MongoClient(mongo_url)
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)
    exported_file = os.path.join(DOWNLOAD_FOLDER, exported_file)
    updated_exported_file = os.path.join(DOWNLOAD_FOLDER, updated_exported_file)
    counter_file = os.path.join(DOWNLOAD_FOLDER, counter_file)
    try:
        mongoDB_database_export_prefect(client, db_name, collection_name, exported_file, s3_backup_bucket, s3_backup_folder)
    except Exception as e:
        log.error(e)
        raise e
    
    try:
        updated_data, counter = update_exported_collection_prefect(exported_file, updated_exported_file, s3_update_reference_file, old_parent_id_field, new_parent_id_field, node, data_commons, s3_backup_bucket, s3_backup_folder)
    except Exception as e:
        log.error(e)
        raise e

    try:
        if updated_data:
            try:
                result = import_collection_prefect(client, db_name, collection_name, updated_data, exported_file, counter, counter_file, s3_backup_bucket, s3_backup_folder)
                if result:
                    log.info("Collection imported successfully")
                else:
                    log.error("Collection import failed, the backup file is loaded to restore the collection")
            except Exception as e:
                log.error(e)
                log.error("Failed to restore the collection from the backup file, please check the backup file and the collection")
                raise e
        else:
            log.info("The updated data is empty or the collection is not found, the collection is not updated")
    except Exception as e:
        log.error(e)
        raise e
    finally:
        client.close()
        if os.path.exists(DOWNLOAD_FOLDER):
            shutil.rmtree(DOWNLOAD_FOLDER)

@flow(name="MongoDB Database Export", log_prints=True)
def mongoDB_database_export_prefect(
    client,
    db_name,
    collection_name,
    exported_file,
    s3_backup_bucket,
    s3_backup_folder
):
    export_collection(client, db_name, collection_name, exported_file)
    upload_s3(s3_backup_bucket, s3_backup_folder, exported_file, log)

@flow(name="Update Exported Collection", log_prints=True)
def update_exported_collection_prefect(
    exported_file,
    updated_exported_file,
    s3_update_reference_file,
    old_parent_id_field,
    new_parent_id_field,
    node,
    data_commons,
    s3_backup_bucket,
    s3_backup_folder
):
    update_reference_file = os.path.join(DOWNLOAD_FOLDER, os.path.basename(s3_update_reference_file))
    download_s3(s3_backup_bucket, s3_update_reference_file, log, update_reference_file)
    updated_data_file, counter = update_exported_collection(exported_file, updated_exported_file, update_reference_file, old_parent_id_field, new_parent_id_field, node, data_commons, log)
    if updated_data_file:
        upload_s3(s3_backup_bucket, s3_backup_folder, updated_exported_file, log)
        return updated_data_file, counter
    else:
        log.info("The updated data is empty or the collection is not found, the collection is not updated")
        return None, None

@flow(name="Import Collection", log_prints=True)
def import_collection_prefect(
    client,
    db_name,
    collection_name,
    updated_data_file,
    exported_file,
    counter,
    counter_file,
    s3_backup_bucket,
    s3_backup_folder
):
    result = import_collection(client, db_name, collection_name, updated_data_file, exported_file, counter, counter_file, log)
    upload_s3(s3_backup_bucket, s3_backup_folder, counter_file, log)
    return result

if __name__ == "__main__":
    mongoDB_database_update_prefect.serve(name="mongoDB_database_update_prefect")