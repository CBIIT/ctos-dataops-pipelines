#!/usr/bin/env python3
"""
Core ICDC data loading logic (TSV files in S3 -> Neo4j via DataLoader).

Ported from the legacy icdc-dataloader repo's loader.py/main(), trimmed to a
plain-dict entry point (icdc_dataloading(argList)) for use by the Prefect
wrapper, since Prefect flow parameters replace the CLI/BentoConfig layer.
"""

import argparse
import glob
import os
import zipfile

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError

from os_loader_icdc_schema import ICDC_Schema
from os_loader_icdc_props import Props
from icdc_data_loader import DataLoader
from bento.common.utils import (
    get_logger,
    removeTrailingSlash,
    check_schema_files,
    UPSERT_MODE,
    DELETE_MODE,
    get_log_file,
    LOG_PREFIX,
    APP_NAME,
    print_config,
)
from bento.common.s3 import S3Bucket, upload_log_file

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = "ICDC_Data_Loader"
    os.environ[APP_NAME] = "ICDC_Data_Loader"

DEFAULT_MAX_VIOLATIONS = 1000000
DEFAULT_TEMP_FOLDER = "tmp"

logger = get_logger("ICDC Data Loader")


def icdc_dataloading(argList: dict):
    """
    Load TSV files from S3 into Neo4j.

    Expected keys in argList:
        neo4j_uri      : bolt://host:port
        neo4j_user     : Neo4j username
        neo4j_password : Neo4j password
        schema_files   : list of paths to model YAML files
        prop_file      : path to properties YAML file
        s3_bucket      : S3 bucket containing the TSV files
        s3_folder      : S3 folder (prefix) containing the TSV files
        dataset        : local directory to download TSVs into (created if missing)
        mode           : 'upsert' (default), 'new', or 'delete'
        cheat_mode     : skip validation (default False)
        dry_run        : validate only, skip loading (default False)
        wipe_db        : wipe database before loading (default False)
        no_parents     : (kept for interface parity; not used by DataLoader.load directly)
        split_transactions: create a separate transaction per file (default False)
        no_backup      : skip Neo4j backup step (default True — backup requires
                         neo4j-admin/SSH access to the DB host, not available from ECS)
        backup_folder  : required if no_backup is False
        max_violations : max validation violations to display (default 1,000,000)
        temp_folder    : local temp folder for validation result files (default "tmp")
        upload_log_dir : s3://bucket/prefix to upload logs to (defaults to s3_bucket/s3_folder/logs)
        verbose        : verbose validation logging (default False)
        plugins        : list of already-instantiated plugin objects (default empty; not yet supported)
    """
    log_file = get_log_file()

    dataset = argList.get("dataset") or "data"
    s3_bucket = argList.get("s3_bucket")
    s3_folder = argList.get("s3_folder")
    schema_files = argList["schema_files"]
    prop_file = argList["prop_file"]
    neo4j_uri = removeTrailingSlash(argList.get("neo4j_uri") or "bolt://localhost:7687")
    neo4j_user = argList.get("neo4j_user") or "neo4j"
    neo4j_password = argList["neo4j_password"]
    mode = argList.get("mode") or UPSERT_MODE
    cheat_mode = argList.get("cheat_mode", False)
    dry_run = argList.get("dry_run", False)
    wipe_db = argList.get("wipe_db", False)
    split_transactions = argList.get("split_transactions", False)
    no_backup = argList.get("no_backup", True)
    backup_folder = argList.get("backup_folder")
    max_violations = argList.get("max_violations") or DEFAULT_MAX_VIOLATIONS
    temp_folder = argList.get("temp_folder") or DEFAULT_TEMP_FOLDER
    verbose = argList.get("verbose", False)
    plugins = argList.get("plugins") or []

    if plugins:
        raise NotImplementedError(
            "Plugin support has not been ported yet; pass an empty plugins list."
        )

    if split_transactions and no_backup:
        raise Exception(
            "split_transactions and no_backup cannot both be enabled, a backup is required "
            "when running in split transactions mode"
        )
    if not backup_folder and not no_backup:
        raise Exception("backup_folder is required unless no_backup is True")

    upload_log_dir = argList.get("upload_log_dir")
    if not upload_log_dir and s3_bucket and s3_folder:
        upload_log_dir = f"s3://{s3_bucket}/{s3_folder}/logs"

    print_config(logger, argList)

    if not check_schema_files(schema_files, logger):
        return False

    if s3_folder:
        if not os.path.exists(dataset):
            os.makedirs(dataset)
        else:
            exist_files = glob.glob("{}/*.txt".format(dataset))
            if len(exist_files) > 0:
                raise Exception(
                    f'Folder: "{dataset}" is not empty, please empty it first'
                )
        if not s3_bucket:
            raise Exception("s3_bucket is required when s3_folder is set")
        bucket = S3Bucket(s3_bucket)
        logger.info(f"Loading data from s3://{s3_bucket}/{s3_folder}")
        if not bucket.download_files_in_folder(s3_folder, dataset):
            raise Exception(f'Download files from S3 bucket "{s3_bucket}" failed!')

    driver = None
    load_result = None
    zip_file_key = None
    try:
        txt_files = glob.glob("{}/*.txt".format(dataset))
        tsv_files = glob.glob("{}/*.tsv".format(dataset))
        file_list = txt_files + tsv_files
        if file_list:
            prop_path = os.path.join(dataset, prop_file)
            props = Props(prop_path if os.path.isfile(prop_path) else prop_file)
            schema = ICDC_Schema(schema_files, props)
            if not dry_run or mode == DELETE_MODE:
                driver = GraphDatabase.driver(
                    neo4j_uri,
                    auth=(neo4j_user, neo4j_password),
                    encrypted=False,
                )

            loader = DataLoader(driver, schema, plugins)

            load_result = loader.load(
                file_list,
                cheat_mode,
                dry_run,
                mode,
                wipe_db,
                max_violations,
                temp_folder,
                verbose,
                split=split_transactions,
                no_backup=no_backup,
                backup_folder=backup_folder,
                neo4j_uri=neo4j_uri,
            )

            if load_result is False:
                if loader.validation_result_file_key:
                    zip_file_key = loader.validation_result_file_key.replace(
                        ".xlsx", ".zip"
                    )
                    with zipfile.ZipFile(zip_file_key, "w") as zipf:
                        zipf.write(
                            loader.validation_result_file_key,
                            os.path.basename(loader.validation_result_file_key),
                        )
                        zipf.write(log_file, os.path.basename(log_file))
                    logger.error(
                        "Data loading failed, validation result zip file was created at {}".format(
                            zip_file_key
                        )
                    )
                else:
                    # No validation report (e.g. index creation or another
                    # non-validation error caused the failure) - still zip
                    # and upload the log so the failure reason is visible.
                    zip_file_key = log_file.replace(".log", ".zip")
                    with zipfile.ZipFile(zip_file_key, "w") as zipf:
                        zipf.write(log_file, os.path.basename(log_file))
                    logger.error("Data loading failed")
            else:
                zip_file_key = log_file.replace(".log", ".zip")
                with zipfile.ZipFile(zip_file_key, "w") as zipf:
                    zipf.write(log_file, os.path.basename(log_file))
                logger.info(
                    "Data loading succeeded, zip file was created at {}".format(
                        zip_file_key
                    )
                )
        else:
            logger.info("No files to load.")

    except ServiceUnavailable:
        logger.critical('Neo4j service not available at: "{}"'.format(neo4j_uri))
        return False
    except AuthError:
        logger.error("Wrong Neo4j username or password!")
        return False
    finally:
        if driver:
            driver.close()

    if upload_log_dir and zip_file_key:
        try:
            upload_log_file(upload_log_dir, zip_file_key)
            logger.info(f"Uploading log/validation zip file {zip_file_key} succeeded!")
        except Exception as e:
            logger.debug(e)
            logger.exception(
                "Uploading log file failed! Check debug log for detailed information"
            )

    if load_result is False:
        raise Exception("Data loading failed; see logs for details")

    return load_result


def main():
    parser = argparse.ArgumentParser(description="Load TSV files from S3 into Neo4j")
    parser.add_argument(
        "config_file", type=argparse.FileType("r"), help="Loader configuration YAML"
    )
    args = parser.parse_args()

    import yaml

    config = yaml.safe_load(args.config_file)["Config"]
    icdc_dataloading(config)


if __name__ == "__main__":
    main()
