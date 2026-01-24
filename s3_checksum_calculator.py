"""
S3 Checksum Calculator - Core Business Logic

This module provides functionality to calculate file sizes and MD5 checksums
for S3 objects at a specified location.

Key Features:
- Support for single files or directory prefixes
- Include/exclude pattern filtering (glob-style)
- Downloads files temporarily to calculate true MD5 checksums
- Uses EFS mount at /usr/local/data for large file operations
- Cleans up all temporary files after operation (success or failure)
- Pagination support for large S3 listings (>1000 objects)

Usage:
    # Single file
    result = s3_checksum_calculator(
        bucket="my-bucket",
        prefix="path/to/file.txt"
    )

    # Directory with filtering
    result = s3_checksum_calculator(
        bucket="my-bucket",
        prefix="data/exports/",
        include_patterns=["*.csv", "*.json"],
        exclude_patterns=["*.log", "temp/*"]
    )
"""

import os
import shutil
import fnmatch
from botocore.exceptions import ClientError
from bento.common.s3 import S3Bucket
from bento.common.utils import get_logger, get_md5, LOG_PREFIX, APP_NAME, get_time_stamp


def format_file_size(size_bytes):
    """
    Convert bytes to human-readable file size format.

    Args:
        size_bytes (int): File size in bytes

    Returns:
        str: Formatted file size (e.g., "150.5 MB", "2.3 GB")
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


def should_process_file(relative_path, include_patterns, exclude_patterns):
    """
    Check if a file should be processed based on include/exclude patterns.

    This function applies filtering logic:
    1. If include_patterns provided: File must match at least one include pattern
    2. If exclude_patterns provided: File must not match any exclude pattern
    3. Include patterns take precedence (applied first)

    Args:
        relative_path (str): File path relative to prefix (e.g., "data/file.txt")
        include_patterns (list): List of glob patterns to include (e.g., ["*.csv", "*.json"])
        exclude_patterns (list): List of glob patterns to exclude (e.g., ["*.log", "temp/*"])

    Returns:
        bool: True if file should be processed, False otherwise

    Examples:
        should_process_file("data/test.csv", ["*.csv"], []) -> True
        should_process_file("data/test.log", ["*.csv"], []) -> False (doesn't match include)
        should_process_file("data/test.csv", ["*.csv"], ["temp/*"]) -> True
        should_process_file("temp/test.csv", ["*.csv"], ["temp/*"]) -> False (matches exclude)
    """
    filename = os.path.basename(relative_path)

    # If include patterns specified, file must match at least one
    if include_patterns:
        include_match = False
        for pattern in include_patterns:
            # Match against full relative path or just filename
            if fnmatch.fnmatch(relative_path, pattern) or fnmatch.fnmatch(filename, pattern):
                include_match = True
                break

        if not include_match:
            return False

    # If exclude patterns specified, file must not match any
    if exclude_patterns:
        for pattern in exclude_patterns:
            # Match against full relative path or just filename
            if fnmatch.fnmatch(relative_path, pattern) or fnmatch.fnmatch(filename, pattern):
                return False

    return True


def is_single_file(bucket_name, prefix, s3_bucket):
    """
    Check if the prefix points to a single file rather than a directory.

    Args:
        bucket_name (str): S3 bucket name
        prefix (str): S3 prefix/key
        s3_bucket (S3Bucket): S3Bucket instance

    Returns:
        tuple: (is_file, metadata_dict or None)
               - is_file (bool): True if prefix is a single file
               - metadata (dict): File metadata if is_file=True, None otherwise
    """
    # If prefix ends with /, it's definitely a directory
    if prefix.endswith("/"):
        return False, None

    # Try to get object metadata
    try:
        response = s3_bucket.client.head_object(Bucket=bucket_name, Key=prefix)
        # If we get here, the object exists
        return True, {
            "Key": prefix,
            "Size": response["ContentLength"],
            "ETag": response["ETag"]
        }
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            # Object doesn't exist, assume it's a prefix
            return False, None
        else:
            # Other error, re-raise
            raise


def list_s3_objects(bucket_name, prefix, s3_bucket, log):
    """
    List all S3 objects under a prefix with pagination support.

    Args:
        bucket_name (str): S3 bucket name
        prefix (str): S3 prefix/folder path
        s3_bucket (S3Bucket): S3Bucket instance
        log: Logger instance

    Returns:
        list: List of object metadata dictionaries with keys: Key, Size, ETag
    """
    # Ensure prefix ends with / for proper listing (unless empty)
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    log.info(f"Listing objects from s3://{bucket_name}/{prefix}")

    all_objects = []
    continuation_token = None

    while True:
        # Make paginated request
        if continuation_token:
            response = s3_bucket.client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                ContinuationToken=continuation_token
            )
        else:
            response = s3_bucket.client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )

        # Collect objects from this page
        if "Contents" in response:
            all_objects.extend(response["Contents"])
            log.info(f"Retrieved {len(response['Contents'])} objects (total so far: {len(all_objects)})")

        # Check if there are more results
        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
            log.info("More objects available, fetching next page...")
        else:
            break

    log.info(f"Total objects found: {len(all_objects)}")
    return all_objects


def calculate_checksums(
    bucket_name,
    prefix,
    objects_to_process,
    temp_dir,
    s3_bucket,
    log
):
    """
    Download files and calculate MD5 checksums.

    Args:
        bucket_name (str): S3 bucket name
        prefix (str): S3 prefix (used for relative path calculation)
        objects_to_process (list): List of object metadata dicts
        temp_dir (str): Temporary directory for downloads
        s3_bucket (S3Bucket): S3Bucket instance
        log: Logger instance

    Returns:
        list: List of file result dictionaries
    """
    results = []
    total_size = 0

    for i, obj in enumerate(objects_to_process, 1):
        s3_key = obj["Key"]
        file_size = obj["Size"]
        etag = obj.get("ETag", "")

        # Skip folder markers
        if s3_key.endswith("/"):
            log.info(f"Skipping folder marker: {s3_key}")
            continue

        # Calculate relative path
        relative_path = s3_key[len(prefix):] if s3_key.startswith(prefix) else s3_key
        if not relative_path:
            relative_path = os.path.basename(s3_key)

        try:
            # Create temporary file path
            local_file_path = os.path.join(temp_dir, f"temp_{i}")

            # Download file
            log.info(f"Downloading {relative_path} ({format_file_size(file_size)})...")
            s3_bucket.download_file(s3_key, local_file_path)

            # Calculate MD5
            md5_hash = get_md5(local_file_path)

            # Add to results
            results.append({
                "key": relative_path,
                "size_bytes": file_size,
                "size_formatted": format_file_size(file_size),
                "md5": md5_hash,
                "etag": etag
            })

            total_size += file_size

            # Clean up immediately to save disk space
            if os.path.exists(local_file_path):
                os.remove(local_file_path)

            # Progress logging
            if i % 50 == 0 or i == len(objects_to_process):
                log.info(
                    f"Progress: {i}/{len(objects_to_process)} files processed "
                    f"({format_file_size(total_size)} total)"
                )

        except Exception as e:
            log.error(f"Failed to process {relative_path}: {e}")
            # Add failed entry
            results.append({
                "key": relative_path,
                "size_bytes": file_size,
                "size_formatted": format_file_size(file_size),
                "md5": "ERROR",
                "etag": etag,
                "error": str(e)
            })
            continue

    return results


def cleanup_temp_directory(temp_dir, log):
    """
    Clean up temporary directory and all its contents.

    Args:
        temp_dir (str): Path to temporary directory
        log: Logger instance
    """
    try:
        if os.path.exists(temp_dir):
            log.info(f"Cleaning up temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir)
            log.info("Cleanup complete")
        else:
            log.info("Temporary directory does not exist, skipping cleanup")
    except Exception as e:
        # Log error but don't raise - cleanup failure is non-fatal
        log.error(f"Failed to clean up temporary directory: {e}")


def s3_checksum_calculator(
    bucket,
    prefix="",
    include_patterns=None,
    exclude_patterns=None
):
    """
    Calculate file sizes and MD5 checksums for S3 objects.

    This function:
    1. Detects if prefix is a single file or directory
    2. Lists S3 objects (with pagination support)
    3. Applies include/exclude filtering
    4. Downloads files to temporary directory
    5. Calculates MD5 checksums
    6. Cleans up temporary files (always runs, even on error)

    Args:
        bucket (str): S3 bucket name
        prefix (str): S3 prefix/key (file or directory path)
        include_patterns (list, optional): Glob patterns to include (e.g., ["*.csv", "*.json"])
        exclude_patterns (list, optional): Glob patterns to exclude (e.g., ["*.log", "temp/*"])

    Returns:
        dict: Result dictionary with keys:
            - status (str): "success" or "failed"
            - error (str): Error message if status is "failed"
            - total_files (int): Number of files processed
            - total_size_bytes (int): Total size in bytes
            - filtered_count (int): Number of files filtered out
            - files (list): List of file result dicts
    """
    # Set up logging
    if LOG_PREFIX not in os.environ:
        os.environ[LOG_PREFIX] = "S3_Checksum_Calculator"
    os.environ[APP_NAME] = "S3_Checksum_Calculator"

    log = get_logger("S3 Checksum Calculator")

    # Convert None to empty list
    if include_patterns is None:
        include_patterns = []
    if exclude_patterns is None:
        exclude_patterns = []

    # Create unique temporary directory
    timestamp = get_time_stamp()
    temp_base_dir = (
        "/usr/local/data"
        if os.path.exists("/usr/local/data") and os.access("/usr/local/data", os.W_OK)
        else "/tmp"
    )
    temp_dir = os.path.join(temp_base_dir, f"s3_checksum_{timestamp}")

    log.info("=" * 80)
    log.info("Starting S3 Checksum Calculation")
    log.info(f"Source: s3://{bucket}/{prefix}")
    if include_patterns:
        log.info(f"Include patterns: {', '.join(include_patterns)}")
    if exclude_patterns:
        log.info(f"Exclude patterns: {', '.join(exclude_patterns)}")
    log.info(f"Temporary directory: {temp_dir}")
    log.info("=" * 80)

    # Initialize result
    result = {
        "status": "failed",
        "error": "",
        "total_files": 0,
        "total_size_bytes": 0,
        "filtered_count": 0,
        "files": []
    }

    try:
        # Create temporary directory
        os.makedirs(temp_dir, exist_ok=True)

        # Initialize S3 connection
        s3_bucket = S3Bucket(bucket)

        # Check if prefix is a single file
        is_file, file_metadata = is_single_file(bucket, prefix, s3_bucket)

        if is_file:
            log.info(f"Detected single file: {prefix}")
            objects = [file_metadata]
            prefix_for_relative_path = os.path.dirname(prefix) + "/" if os.path.dirname(prefix) else ""
        else:
            # List all objects
            objects = list_s3_objects(bucket, prefix, s3_bucket, log)
            prefix_for_relative_path = prefix if prefix.endswith("/") or not prefix else prefix + "/"

        if not objects:
            log.warning("No objects found")
            result["status"] = "success"
            return result

        # Filter objects
        objects_to_process = []
        filtered_count = 0

        for obj in objects:
            s3_key = obj["Key"]

            # Skip folder markers
            if s3_key.endswith("/"):
                continue

            # Calculate relative path for filtering
            relative_path = (
                s3_key[len(prefix_for_relative_path):]
                if s3_key.startswith(prefix_for_relative_path)
                else s3_key
            )

            # Apply filtering
            if should_process_file(relative_path, include_patterns, exclude_patterns):
                objects_to_process.append(obj)
            else:
                filtered_count += 1
                if filtered_count <= 20 or filtered_count % 10 == 0:
                    log.info(f"Filtering out: {relative_path}")

        log.info(f"Objects to process: {len(objects_to_process)}")
        if filtered_count > 0:
            log.info(f"Objects filtered out: {filtered_count}")

        if not objects_to_process:
            log.warning("No objects to process after filtering")
            result["status"] = "success"
            result["filtered_count"] = filtered_count
            return result

        # Calculate checksums
        log.info("Calculating checksums...")
        file_results = calculate_checksums(
            bucket,
            prefix_for_relative_path,
            objects_to_process,
            temp_dir,
            s3_bucket,
            log
        )

        # Calculate totals
        total_size = sum(f["size_bytes"] for f in file_results)

        # Update result
        result["status"] = "success"
        result["total_files"] = len(file_results)
        result["total_size_bytes"] = total_size
        result["filtered_count"] = filtered_count
        result["files"] = file_results

        log.info("=" * 80)
        log.info("S3 Checksum Calculation Completed Successfully")
        log.info(f"Files processed: {result['total_files']}")
        if filtered_count > 0:
            log.info(f"Files filtered: {filtered_count}")
        log.info(f"Total size: {format_file_size(total_size)}")
        log.info("=" * 80)

    except Exception as e:
        result["error"] = str(e)
        log.error("=" * 80)
        log.error(f"S3 Checksum Calculation Failed: {e}")
        log.error("=" * 80)
        raise

    finally:
        # Always clean up temporary files
        log.info("Performing cleanup...")
        cleanup_temp_directory(temp_dir, log)

    return result
