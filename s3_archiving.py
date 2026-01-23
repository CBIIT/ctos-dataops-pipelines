"""
S3 Archiving - Core Business Logic

This module provides functionality to archive files from an S3 location into a zip file
and upload it to a target S3 location. It maintains the S3 folder structure within the zip file.

Key Features:
- Downloads files from source S3 location to EFS temporary directory
- Creates zip archive maintaining folder structure
- Uploads zip file to target S3 location
- Cleans up all temporary files after operation (success or failure)
- Uses EFS mount at /usr/local/data for large file operations

Usage:
    result = s3_archiving(
        source_bucket="my-bucket",
        source_prefix="data/exports/",
        target_bucket="my-bucket",
        target_prefix="archives/",
        zip_filename="my_archive.zip"
    )
"""

import os
import shutil
import zipfile
import fnmatch
from bento.common.s3 import S3Bucket, upload_log_file
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME, get_time_stamp


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


def should_exclude(relative_path, exclude_patterns):
    """
    Check if a file should be excluded based on patterns.

    This function matches the relative file path against a list of glob patterns.
    It checks both the full relative path and just the filename.

    Args:
        relative_path (str): File path relative to source prefix (e.g., "data/file.txt")
        exclude_patterns (list): List of glob patterns (e.g., ["*.log", "temp/*"])

    Returns:
        bool: True if file should be excluded, False otherwise

    Examples:
        should_exclude("data/test.log", ["*.log"]) -> True
        should_exclude("temp/cache.txt", ["temp/*"]) -> True
        should_exclude("data/report.pdf", ["*.log"]) -> False
    """
    if not exclude_patterns:
        return False

    for pattern in exclude_patterns:
        # Match against full relative path
        if fnmatch.fnmatch(relative_path, pattern):
            return True

        # Match against just the filename
        filename = os.path.basename(relative_path)
        if fnmatch.fnmatch(filename, pattern):
            return True

    return False


def download_files_from_s3(s3_bucket, s3_prefix, local_dir, exclude_patterns, log):
    """
    Download all files from S3 location to local directory, maintaining folder structure.

    This function lists all objects under the specified S3 prefix and downloads them
    to the local directory while preserving the S3 folder hierarchy. Files matching
    exclusion patterns are skipped.

    Args:
        s3_bucket (str): S3 bucket name
        s3_prefix (str): S3 prefix/folder path (e.g., "data/exports/")
        local_dir (str): Local directory path for downloads
        exclude_patterns (list): List of glob patterns to exclude (e.g., ["*.log", "temp/*"])
        log: Logger instance for logging progress

    Returns:
        tuple: (file_count, total_size_bytes, excluded_count) - Files downloaded, size, and excluded count

    Raises:
        Exception: If S3 download operation fails
    """
    try:
        # Initialize S3 bucket connection
        bucket = S3Bucket(s3_bucket)

        # Ensure s3_prefix ends with / for proper listing
        if s3_prefix and not s3_prefix.endswith("/"):
            s3_prefix = s3_prefix + "/"

        log.info(f"Listing files from s3://{s3_bucket}/{s3_prefix}")

        # List all objects under the prefix
        response = bucket.client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

        # Check if any files exist
        if "Contents" not in response:
            log.warning(f"No files found in s3://{s3_bucket}/{s3_prefix}")
            return 0, 0

        files = response["Contents"]
        file_count = 0
        excluded_count = 0
        total_size = 0

        log.info(f"Found {len(files)} objects in S3. Starting download...")

        # Download each file
        for i, obj in enumerate(files, 1):
            s3_key = obj["Key"]
            file_size = obj["Size"]

            # Skip if it's a folder marker (key ends with /)
            if s3_key.endswith("/"):
                log.info(f"Skipping folder marker: {s3_key}")
                continue

            # Calculate relative path (remove prefix from key)
            relative_path = (
                s3_key[len(s3_prefix) :] if s3_key.startswith(s3_prefix) else s3_key
            )

            # Skip if relative path is empty
            if not relative_path:
                continue

            # Check if file should be excluded based on patterns
            if should_exclude(relative_path, exclude_patterns):
                excluded_count += 1
                # Log excluded files (first 20, then every 10th)
                if excluded_count <= 20 or excluded_count % 10 == 0:
                    log.info(f"Excluding file (pattern match): {relative_path}")
                continue

            # Construct local file path
            local_file_path = os.path.join(local_dir, relative_path)

            # Create subdirectories if needed
            local_file_dir = os.path.dirname(local_file_path)
            if local_file_dir and not os.path.exists(local_file_dir):
                os.makedirs(local_file_dir)

            # Download file
            bucket.download_file(s3_key, local_file_path)
            file_count += 1
            total_size += file_size

            # Log progress every 10 files or at completion
            if i % 10 == 0 or i == len(files):
                log.info(
                    f"Downloaded {file_count} of {len(files)} files ({format_file_size(total_size)} total)"
                )

        log.info(
            f"Download complete: {file_count} files, {format_file_size(total_size)}"
        )
        if excluded_count > 0:
            log.info(f"Excluded {excluded_count} files based on patterns")
        return file_count, total_size, excluded_count

    except Exception as e:
        log.error(f"Failed to download files from S3: {e}")
        raise


def create_zip_archive(source_dir, zip_path, log):
    """
    Create a zip archive from a directory, maintaining folder structure.

    This function walks through the source directory and adds all files to a zip archive
    while preserving the relative directory structure.

    Args:
        source_dir (str): Source directory path containing files to zip
        zip_path (str): Target zip file path
        log: Logger instance for logging progress

    Returns:
        int: Size of created zip file in bytes

    Raises:
        Exception: If zip creation fails
    """
    try:
        log.info(f"Creating zip archive: {zip_path}")

        file_count = 0

        # Create zip file with compression
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            # Walk through source directory
            for root, dirs, files in os.walk(source_dir):
                for file in files:
                    # Get full file path
                    file_path = os.path.join(root, file)

                    # Calculate relative path from source_dir
                    arcname = os.path.relpath(file_path, source_dir)

                    # Add file to zip
                    zipf.write(file_path, arcname)
                    file_count += 1

                    # Log progress every 50 files
                    if file_count % 50 == 0:
                        log.info(f"Added {file_count} files to zip archive")

        # Get zip file size
        zip_size = os.path.getsize(zip_path)

        log.info(
            f"Zip archive created: {file_count} files, {format_file_size(zip_size)}"
        )
        return zip_size

    except Exception as e:
        log.error(f"Failed to create zip archive: {e}")
        raise


def upload_zip_to_s3(s3_bucket, s3_folder, zip_file_path, log):
    """
    Upload zip file to S3 target location.

    Args:
        s3_bucket (str): Target S3 bucket name
        s3_folder (str): Target S3 folder/prefix
        zip_file_path (str): Local path to zip file
        log: Logger instance for logging progress

    Returns:
        str: S3 URI of uploaded file (e.g., "s3://bucket/folder/file.zip")

    Raises:
        Exception: If S3 upload operation fails
    """
    try:
        # Get filename from path
        zip_filename = os.path.basename(zip_file_path)

        # Format S3 destination
        # Ensure folder ends with / if it's not empty
        if s3_folder and not s3_folder.endswith("/"):
            s3_folder = s3_folder + "/"

        dest = f"s3://{s3_bucket}/{s3_folder}{zip_filename}"

        log.info(f"Uploading zip file to {dest}")

        # Get file size for logging
        file_size = os.path.getsize(zip_file_path)

        # Upload to S3 using Bento utility
        # Strip trailing slash to prevent double slash (e.g., "folder//file.zip") which creates "/" folder
        upload_log_file(f"s3://{s3_bucket}/{s3_folder.rstrip('/')}", zip_file_path)

        log.info(f"Upload complete: {dest} ({format_file_size(file_size)})")
        return dest

    except Exception as e:
        log.error(f"Failed to upload zip file to S3: {e}")
        raise


def cleanup_temp_files(temp_dir, log):
    """
    Clean up temporary directory and all its contents.

    This function removes the entire temporary directory tree. Errors during cleanup
    are logged but do not raise exceptions (non-fatal cleanup).

    Args:
        temp_dir (str): Path to temporary directory to remove
        log: Logger instance for logging cleanup status
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


def s3_archiving(
    source_bucket, source_prefix, target_bucket, target_prefix, zip_filename, exclude_patterns=None
):
    """
    Main function to archive S3 files into a zip and upload to target location.

    This function orchestrates the complete archiving workflow:
    1. Download files from source S3 location to EFS temp directory (excluding pattern matches)
    2. Create zip archive maintaining folder structure
    3. Upload zip file to target S3 location
    4. Clean up temporary files (always runs, even on error)

    Args:
        source_bucket (str): Source S3 bucket name
        source_prefix (str): Source S3 prefix/folder path
        target_bucket (str): Target S3 bucket name
        target_prefix (str): Target S3 prefix/folder path
        zip_filename (str): Name for the zip file (with .zip extension)
        exclude_patterns (list, optional): List of glob patterns to exclude (e.g., ["*.log", "temp/*"])

    Returns:
        dict: Result dictionary with keys:
            - status (str): "success" or "failed"
            - file_count (int): Number of files archived
            - excluded_count (int): Number of files excluded
            - zip_size (int): Size of zip file in bytes
            - s3_path (str): S3 URI of uploaded zip file
            - error (str): Error message if status is "failed"
    """
    # Set up logging environment
    if LOG_PREFIX not in os.environ:
        os.environ[LOG_PREFIX] = "S3_Archiving"
    os.environ[APP_NAME] = "S3_Archiving"

    log = get_logger("S3 Archiving")

    # Convert None to empty list
    if exclude_patterns is None:
        exclude_patterns = []

    # Create unique temporary directory
    # Use EFS mount in production (/usr/local/data), fallback to /tmp for local testing
    timestamp = get_time_stamp()
    temp_base_dir = (
        "/usr/local/data"
        if os.path.exists("/usr/local/data") and os.access("/usr/local/data", os.W_OK)
        else "/tmp"
    )
    temp_dir = os.path.join(temp_base_dir, f"s3_archive_{timestamp}")

    log.info("=" * 80)
    log.info("Starting S3 Archiving Job")
    log.info(f"Source: s3://{source_bucket}/{source_prefix}")
    log.info(f"Target: s3://{target_bucket}/{target_prefix}/{zip_filename}")
    log.info(f"Temporary directory: {temp_dir}")
    log.info("=" * 80)

    # Initialize result dictionary
    result = {
        "status": "failed",
        "file_count": 0,
        "excluded_count": 0,
        "zip_size": 0,
        "s3_path": "",
        "error": "",
    }

    try:
        # Create temporary directory
        os.makedirs(temp_dir, exist_ok=True)

        # Step 1: Download files from S3
        log.info("Step 1/3: Downloading files from S3...")
        download_dir = os.path.join(temp_dir, "downloads")
        os.makedirs(download_dir, exist_ok=True)

        file_count, total_size, excluded_count = download_files_from_s3(
            source_bucket, source_prefix, download_dir, exclude_patterns, log
        )

        if file_count == 0:
            raise Exception("No files found to archive (all files may be excluded)")

        # Step 2: Create zip archive
        log.info("Step 2/3: Creating zip archive...")
        zip_path = os.path.join(temp_dir, zip_filename)
        zip_size = create_zip_archive(download_dir, zip_path, log)

        # Step 3: Upload to S3
        log.info("Step 3/3: Uploading zip file to S3...")
        s3_path = upload_zip_to_s3(target_bucket, target_prefix, zip_path, log)

        # Update result with success
        result["status"] = "success"
        result["file_count"] = file_count
        result["excluded_count"] = excluded_count
        result["zip_size"] = zip_size
        result["s3_path"] = s3_path

        log.info("=" * 80)
        log.info("S3 Archiving Job Completed Successfully")
        log.info(f"Files archived: {file_count}")
        if excluded_count > 0:
            log.info(f"Files excluded: {excluded_count}")
        log.info(f"Zip size: {format_file_size(zip_size)}")
        log.info(f"Location: {s3_path}")
        log.info("=" * 80)

    except Exception as e:
        result["error"] = str(e)
        log.error("=" * 80)
        log.error(f"S3 Archiving Job Failed: {e}")
        log.error("=" * 80)
        raise

    finally:
        # Always clean up temporary files, even on error
        log.info("Performing cleanup...")
        cleanup_temp_files(temp_dir, log)

    return result
