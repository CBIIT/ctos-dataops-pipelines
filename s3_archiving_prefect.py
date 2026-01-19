"""
S3 Archiving - Prefect Flow Wrapper

This module provides the Prefect flow wrapper for the S3 archiving functionality.
It orchestrates the core archiving logic with Prefect decorators, parameter management,
and artifact creation for the Prefect UI.

Usage:
    # Deploy to Prefect
    prefect deploy --all --prefect-file config/s3_archiving_prefect.yaml

    # Run locally for testing
    python s3_archiving_prefect.py
"""

from prefect import flow
from s3_archiving import s3_archiving, format_file_size
from bento.common.utils import get_time_stamp
from prefect.artifacts import create_markdown_artifact


def create_summary_markdown(result, source_bucket, source_prefix, target_bucket, target_prefix):
    """
    Create markdown summary for Prefect artifact.

    Args:
        result (dict): Result dictionary from s3_archiving function
        source_bucket (str): Source S3 bucket name
        source_prefix (str): Source S3 prefix
        target_bucket (str): Target S3 bucket name
        target_prefix (str): Target S3 prefix

    Returns:
        str: Formatted markdown summary
    """
    # Determine status emoji and message
    if result['status'] == 'success':
        status_emoji = '✅'
        status_text = 'Success'
    else:
        status_emoji = '❌'
        status_text = 'Failed'

    # Build markdown summary
    summary = f'''# S3 Archiving Summary

## Status
{status_emoji} **{status_text}**

## Source Location
```
s3://{source_bucket}/{source_prefix}
```

## Target Location
```
{result.get('s3_path', f"s3://{target_bucket}/{target_prefix}/")}
```

## Archive Details

| Metric | Value |
|:-------|------:|
| Files Archived | {result.get('file_count', 0):,} |
| Zip File Size | {format_file_size(result.get('zip_size', 0))} |
| Status | {status_text} |
'''

    # Add error information if failed
    if result['status'] == 'failed' and result.get('error'):
        summary += f'''
## Error Details
```
{result['error']}
```
'''

    return summary


@flow(name="s3 archiving", log_prints=True)
def s3_archiving_prefect(
    source_bucket: str,
    source_prefix: str,
    target_bucket: str,
    target_prefix: str,
    zip_filename: str = ""
):
    """
    Prefect flow for archiving S3 files into a zip and uploading to target location.

    This flow downloads files from a source S3 location, creates a zip archive
    maintaining the folder structure, and uploads it to a target S3 location.
    Temporary files are automatically cleaned up after the operation.

    Args:
        source_bucket (str): Source S3 bucket name (e.g., "my-data-bucket")
        source_prefix (str): Source S3 prefix/folder path (e.g., "data/exports/")
        target_bucket (str): Target S3 bucket name (e.g., "my-archive-bucket")
        target_prefix (str): Target S3 prefix/folder path (e.g., "archives/2024/")
        zip_filename (str, optional): Name for zip file. Auto-generated if not provided.

    Returns:
        dict: Result dictionary with status, file_count, zip_size, and s3_path
    """
    print("=" * 80)
    print("Starting S3 Archiving Flow")
    print(f"Source: s3://{source_bucket}/{source_prefix}")
    print(f"Target: s3://{target_bucket}/{target_prefix}")
    print("=" * 80)

    # Generate timestamp-based filename if not provided
    if not zip_filename or zip_filename.strip() == "":
        timestamp = get_time_stamp()
        zip_filename = f"s3_archive_{timestamp}.zip"
        print(f"Generated zip filename: {zip_filename}")

    # Ensure filename ends with .zip
    if not zip_filename.endswith('.zip'):
        zip_filename = f"{zip_filename}.zip"
        print(f"Added .zip extension: {zip_filename}")

    # Call core archiving function
    print("\nExecuting archiving operation...")
    result = s3_archiving(
        source_bucket=source_bucket,
        source_prefix=source_prefix,
        target_bucket=target_bucket,
        target_prefix=target_prefix,
        zip_filename=zip_filename
    )

    # Create markdown artifact for Prefect UI
    summary_markdown = create_summary_markdown(
        result,
        source_bucket,
        source_prefix,
        target_bucket,
        target_prefix
    )

    create_markdown_artifact(
        key="s3-archiving-summary",
        markdown=summary_markdown,
        description="S3 Archiving Operation Summary"
    )

    # Print completion message
    print("\n" + "=" * 80)
    if result['status'] == 'success':
        print("✅ S3 Archiving Flow Completed Successfully")
        print(f"Files archived: {result['file_count']:,}")
        print(f"Zip size: {format_file_size(result['zip_size'])}")
        print(f"Location: {result['s3_path']}")
    else:
        print("❌ S3 Archiving Flow Failed")
        if result.get('error'):
            print(f"Error: {result['error']}")
    print("=" * 80)

    return result


if __name__ == "__main__":
    # Serve the flow for local testing
    # To run: python s3_archiving_prefect.py
    # Then trigger from Prefect UI or CLI
    s3_archiving_prefect.serve(name="s3-archiving")
