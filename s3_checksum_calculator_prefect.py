"""
S3 Checksum Calculator - Prefect Flow Wrapper

This module provides the Prefect flow wrapper for the S3 checksum calculator functionality.
It orchestrates the core checksum calculation logic with Prefect decorators, parameter management,
and artifact creation for the Prefect UI.

Usage:
    # Deploy to Prefect
    prefect deploy --prefect-file config/s3_checksum_calculator_prefect.yaml

    # Run locally for testing
    python s3_checksum_calculator_prefect.py
"""

from prefect import flow
from s3_checksum_calculator import s3_checksum_calculator, format_file_size
from prefect.artifacts import create_markdown_artifact


def create_checksum_markdown(result, bucket, prefix):
    """
    Create markdown summary for Prefect artifact.

    Args:
        result (dict): Result dictionary from s3_checksum_calculator function
        bucket (str): S3 bucket name
        prefix (str): S3 prefix

    Returns:
        str: Formatted markdown summary
    """
    # Determine status emoji and message
    if result["status"] == "success":
        status_emoji = "✅"
        status_text = "Success"
    else:
        status_emoji = "❌"
        status_text = "Failed"

    # Build markdown summary
    source_location = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}/"

    summary = f"""# S3 Checksum Calculation Summary

## Status
{status_emoji} **{status_text}**

## Source Location
```
{source_location}
```

## Summary

| Metric | Value |
|:-------|------:|
| Files Processed | {result.get('total_files', 0):,} |
| Files Filtered Out | {result.get('filtered_count', 0):,} |
| Total Size | {format_file_size(result.get('total_size_bytes', 0))} |
| Status | {status_text} |
"""

    # Add file details table
    files = result.get("files", [])
    if files:
        # Limit to first 100 files for readability
        display_files = files[:100]
        has_more = len(files) > 100

        summary += "\n## File Details\n\n"

        if has_more:
            summary += f"*(Showing first 100 of {len(files):,} files. Check flow logs for complete list.)*\n\n"

        summary += "| File Path | Size | Size (bytes) | MD5 Checksum |\n"
        summary += "|:----------|-----:|-------------:|:-------------|\n"

        for file_info in display_files:
            file_key = file_info.get("key", "")
            file_size = file_info.get("size_formatted", "")
            file_size_bytes = file_info.get("size_bytes", 0)
            md5 = file_info.get("md5", "")

            # Truncate long paths for display
            display_key = file_key if len(file_key) <= 60 else "..." + file_key[-57:]

            # Handle errors
            if "error" in file_info:
                md5_display = f"❌ {file_info.get('error', 'Unknown error')}"
            else:
                md5_display = f"`{md5}`"

            summary += f"| {display_key} | {file_size} | {file_size_bytes:,} | {md5_display} |\n"

    # Add error information if failed
    if result["status"] == "failed" and result.get("error"):
        summary += f"""
## Error Details
```
{result['error']}
```
"""

    return summary


@flow(name="s3 checksum calculator", log_prints=True)
def s3_checksum_calculator_prefect(
    bucket: str,
    prefix: str = "",
    include_patterns: str = "",
    exclude_patterns: str = ""
):
    """
    Prefect flow for calculating file sizes and MD5 checksums for S3 objects.

    This flow processes S3 objects at a specified location, calculating their sizes
    and MD5 checksums. It supports single files, multiple files, or entire prefixes.
    Files can be filtered using include/exclude patterns.

    Args:
        bucket (str): S3 bucket name (e.g., "my-data-bucket")
        prefix (str, optional): S3 prefix/key (file or directory path, e.g., "data/exports/" or "data/file.txt")
        include_patterns (str, optional): Comma-separated glob patterns to include (e.g., "*.csv,*.json")
        exclude_patterns (str, optional): Comma-separated glob patterns to exclude (e.g., "*.log,temp/*")

    Returns:
        dict: Result dictionary with status, total_files, total_size_bytes, filtered_count, and files list
    """
    print("=" * 80)
    print("Starting S3 Checksum Calculator Flow")
    print(f"Bucket: {bucket}")
    if prefix:
        print(f"Prefix: {prefix}")
    print("=" * 80)

    # Parse comma-separated patterns into lists
    include_list = []
    if include_patterns and include_patterns.strip():
        include_list = [p.strip() for p in include_patterns.split(",") if p.strip()]
        print(f"Include patterns: {', '.join(include_list)}")

    exclude_list = []
    if exclude_patterns and exclude_patterns.strip():
        exclude_list = [p.strip() for p in exclude_patterns.split(",") if p.strip()]
        print(f"Exclude patterns: {', '.join(exclude_list)}")

    # Call core checksum calculator function
    print("\nExecuting checksum calculation...")
    result = s3_checksum_calculator(
        bucket=bucket,
        prefix=prefix,
        include_patterns=include_list,
        exclude_patterns=exclude_list
    )

    # Create markdown artifact for Prefect UI
    print("\nCreating markdown artifact...")
    try:
        summary_markdown = create_checksum_markdown(result, bucket, prefix)
        print(f"Markdown length: {len(summary_markdown)} characters")

        create_markdown_artifact(
            key="s3-checksum-summary",
            markdown=summary_markdown,
            description="S3 Checksum Calculation Summary"
        )

        print("✅ Markdown artifact created successfully")
    except Exception as e:
        print(f"⚠️  Warning: Failed to create markdown artifact: {e}")
        print("Flow will continue, but artifact will not be available in UI")

    # Print completion message
    print("\n" + "=" * 80)
    if result["status"] == "success":
        print("✅ S3 Checksum Calculator Flow Completed Successfully")
        print(f"Files processed: {result['total_files']:,}")
        if result.get("filtered_count", 0) > 0:
            print(f"Files filtered: {result['filtered_count']:,}")
        print(f"Total size: {format_file_size(result['total_size_bytes'])}")

        # Print file details
        files = result.get("files", [])
        if files:
            print("\n" + "-" * 80)
            print("FILE DETAILS:")
            print("-" * 80)
            for i, file_info in enumerate(files, 1):
                print(f"\n{i}. {file_info.get('key', 'Unknown')}")
                print(f"   Size: {file_info.get('size_formatted', 'Unknown')} ({file_info.get('size_bytes', 0):,} bytes)")
                print(f"   MD5:  {file_info.get('md5', 'Unknown')}")
                if "error" in file_info:
                    print(f"   ⚠️  Error: {file_info.get('error')}")
            print("-" * 80)
    else:
        print("❌ S3 Checksum Calculator Flow Failed")
        if result.get("error"):
            print(f"Error: {result['error']}")
    print("=" * 80)

    return result


if __name__ == "__main__":
    # Serve the flow for local testing
    # To run: python s3_checksum_calculator_prefect.py
    # Then trigger from Prefect UI or CLI
    s3_checksum_calculator_prefect.serve(name="s3-checksum-calculator")
