"""
S3 Checksum Calculator - Local Execution Wrapper

This module provides a command-line interface for calculating file sizes and MD5 checksums
for S3 objects. It wraps the core s3_checksum_calculator function for standalone execution
without Prefect.

Usage:
    # Single file
    python s3_checksum_calculator_local.py --bucket my-bucket --prefix path/to/file.txt

    # All files in a directory
    python s3_checksum_calculator_local.py --bucket my-bucket --prefix data/exports/

    # With filtering
    python s3_checksum_calculator_local.py \\
        --bucket my-bucket \\
        --prefix data/ \\
        --include-patterns "*.csv,*.json" \\
        --exclude-patterns "*.log,temp/*"

    # With JSON output
    python s3_checksum_calculator_local.py \\
        --bucket my-bucket \\
        --prefix data/file.csv \\
        --output-json results.json
"""

import argparse
import json
import sys
from s3_checksum_calculator import s3_checksum_calculator, format_file_size


def print_summary(result):
    """
    Print human-readable summary to stdout.

    Args:
        result (dict): Result dictionary from s3_checksum_calculator
    """
    print("\n" + "=" * 80)
    print("S3 Checksum Calculation Summary")
    print("=" * 80)

    if result["status"] == "success":
        print(f"Status: ✅ Success")
        print(f"Files processed: {result['total_files']:,}")
        if result.get("filtered_count", 0) > 0:
            print(f"Files filtered: {result['filtered_count']:,}")
        print(f"Total size: {format_file_size(result['total_size_bytes'])}")

        # Print file details (first 20 files)
        files = result.get("files", [])
        if files:
            print("\nFile Details (showing first 20):")
            print("-" * 80)
            for i, file_info in enumerate(files[:20], 1):
                print(f"{i}. {file_info['key']}")
                print(f"   Size: {file_info['size_formatted']}")
                print(f"   MD5:  {file_info['md5']}")
                if "error" in file_info:
                    print(f"   Error: {file_info['error']}")
                print()

            if len(files) > 20:
                print(f"... and {len(files) - 20} more files")
                print(f"(Use --output-json to save complete results)")

    else:
        print(f"Status: ❌ Failed")
        if result.get("error"):
            print(f"Error: {result['error']}")

    print("=" * 80)


def main():
    """
    Main entry point for local execution.
    """
    parser = argparse.ArgumentParser(
        description="Calculate file sizes and MD5 checksums for S3 objects",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single file
  %(prog)s --bucket my-bucket --prefix path/to/file.txt

  # All files in a directory
  %(prog)s --bucket my-bucket --prefix data/

  # With filtering
  %(prog)s --bucket my-bucket --prefix data/ \\
      --include-patterns "*.csv,*.json" \\
      --exclude-patterns "*.log,temp/*"

  # With JSON output
  %(prog)s --bucket my-bucket --prefix data/ --output-json results.json
        """
    )

    parser.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket name"
    )

    parser.add_argument(
        "--prefix",
        default="",
        help="S3 prefix/key (file or directory path). Leave empty for bucket root."
    )

    parser.add_argument(
        "--include-patterns",
        default="",
        help='Comma-separated glob patterns to include (e.g., "*.csv,*.json")'
    )

    parser.add_argument(
        "--exclude-patterns",
        default="",
        help='Comma-separated glob patterns to exclude (e.g., "*.log,temp/*")'
    )

    parser.add_argument(
        "--output-json",
        help="Output file path for JSON results (optional)"
    )

    args = parser.parse_args()

    # Parse patterns
    include_list = []
    if args.include_patterns.strip():
        include_list = [p.strip() for p in args.include_patterns.split(",") if p.strip()]

    exclude_list = []
    if args.exclude_patterns.strip():
        exclude_list = [p.strip() for p in args.exclude_patterns.split(",") if p.strip()]

    # Print configuration
    print("Configuration:")
    print(f"  Bucket: {args.bucket}")
    print(f"  Prefix: {args.prefix if args.prefix else '(root)'}")
    if include_list:
        print(f"  Include patterns: {', '.join(include_list)}")
    if exclude_list:
        print(f"  Exclude patterns: {', '.join(exclude_list)}")

    # Run checksum calculation
    try:
        result = s3_checksum_calculator(
            bucket=args.bucket,
            prefix=args.prefix,
            include_patterns=include_list,
            exclude_patterns=exclude_list
        )

        # Print summary
        print_summary(result)

        # Write JSON output if requested
        if args.output_json:
            with open(args.output_json, "w") as f:
                json.dump(result, f, indent=2)
            print(f"\nFull results written to: {args.output_json}")

        # Exit with appropriate code
        sys.exit(0 if result["status"] == "success" else 1)

    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
