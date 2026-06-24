import boto3
from bento.common.utils import get_logger, get_time_stamp
from common.md5_calculator import calculate_file_md5
from bento.common.s3 import S3Bucket, upload_log_file
import os
import pandas as pd
import time
import hashlib

FILE_SEP = "/"
S3_PREFIX = "s3://"
FILE_URL_IN_CDS = "file_url_in_cds"
MD5SUM = "md5sum"
FILE_NAME = "file_name"
FILE_SIZE = "file_size"
FILE_TYPE = "file_type"
METADATA_DIR = "metadata"
TEMP_DOWNLOAD_DIR = "/tmp/download"
timestamp = get_time_stamp()

def compute_s3_md5(s3, bucket: str, key: str, chunk_size: int = 64 * 1024 * 1024):
    

    # Get total file size
    #s3 = boto3.client("s3", config=config)
    head = s3.head_object(Bucket=bucket, Key=key)
    total_size = head["ContentLength"]

    md5_hash = hashlib.md5()
    offset = 0

    while offset < total_size:
        end = min(offset + chunk_size - 1, total_size - 1)
        range_header = f"bytes={offset}-{end}"

        # Each range request is independent - retryable
        response = s3.get_object(Bucket=bucket, Key=key, Range=range_header)
        chunk = response["Body"].read()
        md5_hash.update(chunk)

        offset += len(chunk)
        print(f"Progress: {offset / total_size * 100:.1f}%")

    return md5_hash.hexdigest()

def upload_s3(s3_prefix, s3_bucket, file_key, log):
    dest = os.path.join(f"s3://{s3_bucket}", s3_prefix)
    if dest.endswith(FILE_SEP):
        dest = dest[:-1]
    log.info(f'Exported memgraph file successfully to {file_key}, now start uploading the memgraph export file to {dest}')
    upload_log_file(dest, file_key)
    log.info(f'Uploading the memgraph export file {os.path.basename(file_key)} succeeded!')

def get_all_files_and_metadata(bucket_name, directory_prefix="", tsv_filename="", log=None):
    """
    Lists all files under a specified S3 directory prefix and retrieves metadata for each.
    """
    if directory_prefix.endswith(FILE_SEP):
        directory_prefix = directory_prefix[:-1]
    s3_config = boto3.config.Config(
        read_timeout=300, # 5 min per chunk read
        connect_timeout=30,
        retries={"max_attempts": 5, "mode": "adaptive"},
        )
    s3_client = boto3.client('s3', config=s3_config)
    s3_bucket = S3Bucket(bucket_name)
    paginator = s3_client.get_paginator('list_objects_v2')
    prefix = directory_prefix + FILE_SEP if directory_prefix else ""
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    tsv_s3_key = os.path.join(directory_prefix, METADATA_DIR, tsv_filename)
    tsv_s3_url = f"{S3_PREFIX}{bucket_name}{FILE_SEP}{tsv_s3_key}"
    tsv_s3_folder = os.path.join(directory_prefix, METADATA_DIR)
    # try download file from s3 bucket to using filename if file exist in s3 bucket
    try:
        if s3_bucket.file_exists_on_s3(tsv_s3_key):
            s3_bucket.download_file(tsv_s3_key, tsv_filename)
            file_metadata_df = pd.read_csv(tsv_filename, delimiter='\t')
            # check if the columns are correct with the expected columns
            if not all(col in file_metadata_df.columns for col in [FILE_URL_IN_CDS, MD5SUM, FILE_NAME, FILE_SIZE, FILE_TYPE]):
                # show missing columns
                missing_columns = [col for col in [FILE_URL_IN_CDS, MD5SUM, FILE_NAME, FILE_SIZE, FILE_TYPE] if col not in file_metadata_df.columns]
                log.error(f"The columns in the file {tsv_filename} are not correct, missing columns: {missing_columns}")
                return False
        else:
            log.info(f"File {tsv_filename} does not exist in s3 bucket, creating a new file with the expected columns")
            file_metadata_df = pd.DataFrame(columns=[FILE_URL_IN_CDS, MD5SUM, FILE_NAME, FILE_SIZE, FILE_TYPE])
    except Exception as e:
        log.error(f"Failed to download file {tsv_filename} from s3 bucket: {e}")
        return False
    
    original_file_metadata_df_len = len(file_metadata_df)
    # timming the execution time
    start_time = time.time()
    #set up failed files dataframe
    failed_files_df = pd.DataFrame(columns=[FILE_URL_IN_CDS])
    failed_files_count = 0
    failed_files_name = f"{tsv_filename}_error_files_{timestamp}.tsv"
    # create temp download directory if not exists
    os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
    # If any files in TEMP_DOWNLOAD_DIR delete them
    for file in os.listdir(TEMP_DOWNLOAD_DIR):
        os.remove(os.path.join(TEMP_DOWNLOAD_DIR, file))
        log.info(f"Deleted file {file} from {TEMP_DOWNLOAD_DIR}")

    try:
        for page in pages:
            if 'Contents' in page:
                for file_object in page['Contents']:
                    #skip if file is a directory
                    object_key = file_object['Key']
                    if object_key == directory_prefix and directory_prefix.endswith('/'):
                        continue
                    #check if file already mapped
                    file_url = f"{S3_PREFIX}{bucket_name}{FILE_SEP}{object_key}"
                    if len(file_metadata_df) > 0:
                        if file_url in file_metadata_df[FILE_URL_IN_CDS].tolist() or file_url == tsv_s3_url:
                            log.info(f"File {file_url} already mapped")
                            continue
                    log.info(f"Retrieving metadata for: {object_key}")
                    try:
                        metadata_response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
                        file_size = metadata_response.get('ContentLength')
                        s3_hash = compute_s3_md5(s3_client, bucket_name, object_key)
                        # download file from s3 bucket to local file
                        #local_file_path = os.path.join(TEMP_DOWNLOAD_DIR, os.path.basename(object_key))
                        #try:
                            #result, msg = s3_bucket.download_file(object_key, local_file_path)
                            #if not result:
                            #    log.error(f"Failed to download file {object_key} from s3 bucket: {msg}")
                            #    failed_files_df_new_row = pd.DataFrame([{FILE_URL_IN_CDS: file_url}])
                            #    failed_files_df = pd.concat([failed_files_df, failed_files_df_new_row], ignore_index=True)
                            #    failed_files_count += 1
                            #    continue
                            #else:
                            #    log.info(f"Downloading file {object_key} from s3 bucket succeeded!")
                            #    s3_hash = calculate_file_md5(local_file_path, file_size, log)
                            #    log.info(f"Calculating file {object_key} md5 succeeded!")
                            #except Exception as e:
                            #    log.error(f"Failed to download file {object_key} from s3 bucket: {e}")
                            #    failed_files_df_new_row = pd.DataFrame([{FILE_URL_IN_CDS: file_url}])
                            #    failed_files_df = pd.concat([failed_files_df, failed_files_df_new_row], ignore_index=True)
                            #    failed_files_count += 1
                            #    continue
                        # no matter success or failed, delete local file
                        #if os.path.exists(local_file_path):
                        #    os.remove(local_file_path)
                        # Extract common useful fields
                        extracted_data = {
                            FILE_URL_IN_CDS: file_url,
                            MD5SUM: s3_hash,
                            #'Key': object_key,
                            FILE_NAME: object_key if directory_prefix == "" else object_key.replace(directory_prefix+FILE_SEP,""),
                            FILE_SIZE: file_size,
                            FILE_TYPE: os.path.splitext(object_key)[1].lstrip('.').upper()
                            #'LastModified': metadata_response.get('LastModified'),
                            #'ETag': metadata_response.get('ETag').strip('"'), # Remove quotes from ETag
                            #'ContentType': metadata_response.get('ContentType'),
                            # Add custom metadata fields if you use them (e.g., metadata_response.get('Metadata', {}).get('my-custom-header'))
                        }
                        file_metadata_df_new_row = pd.DataFrame([extracted_data])
                        file_metadata_df = pd.concat([file_metadata_df, file_metadata_df_new_row], ignore_index=True)
                        #write to tsv file after each for loop
                        file_metadata_df.to_csv(tsv_filename, index=False, sep='\t')

                        #upload tsv file to s3 bucket every 60 minutes
                        if time.time() - start_time > 3600:
                            if len(file_metadata_df) > original_file_metadata_df_len:
                                upload_s3(tsv_s3_folder, bucket_name, tsv_filename, log)
                            if len(failed_files_df) > 0:
                                upload_s3(tsv_s3_folder, bucket_name, failed_files_name, log)
                            log.info(f"Uploading the tsv files succeeded after 60 minutes!")
                            start_time = time.time()
                
                    except Exception as e:
                        log.error(f"Failed to retrieve metadata for {object_key}: {e}")
                        failed_files_df_new_row = pd.DataFrame([{FILE_URL_IN_CDS: file_url}])
                        failed_files_df = pd.concat([failed_files_df, failed_files_df_new_row], ignore_index=True)
                        failed_files_count += 1
                        failed_files_df.to_csv(failed_files_name, index=False, sep='\t')
    except Exception as e:
        log.error(f"An unexpected error occurred during file writing: {e}")
        if len(file_metadata_df) > original_file_metadata_df_len:
            upload_s3(tsv_s3_folder, bucket_name, tsv_filename, log)
            log.info(f"Uploading the tsv files {os.path.basename(tsv_filename)} to s3://{bucket_name}/{tsv_s3_folder} succeeded!")
        if len(failed_files_df) > 0:
            upload_s3(tsv_s3_folder, bucket_name, failed_files_name, log)
            log.info(f"Uploading the failed files {os.path.basename(failed_files_name)} to s3://{bucket_name}/{tsv_s3_folder} succeeded!, total failed files: {failed_files_count}")
        return False
                
            
    #upload tsv file to s3 bucket
    if len(file_metadata_df) > original_file_metadata_df_len:
        upload_s3(tsv_s3_folder, bucket_name, tsv_filename, log)
        log.info(f"Uploading the tsv files {os.path.basename(tsv_filename)} to s3://{bucket_name}/{tsv_s3_folder} succeeded!")
    if len(failed_files_df) > 0:
        upload_s3(tsv_s3_folder, bucket_name, failed_files_name, log)
        log.info(f"Uploading the failed files {os.path.basename(failed_files_name)} succeeded!, total failed files: {failed_files_count}")
    else:
        log.info(f"No failed files found, total failed files: {failed_files_count}")
    return True

# --- Example Usage ---
if __name__ == "__main__":
    bucket_name = ""  # Replace with your S3 bucket name
    s3_directory_prefix = "" # e.g., 'images/' or '' for the whole bucket
    tsv_filename = ""  # Local filename for the TSV output
    log = get_logger('S3 File Metadata Generator')
    result = get_all_files_and_metadata(bucket_name, s3_directory_prefix, tsv_filename, log)
