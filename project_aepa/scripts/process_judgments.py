#!/usr/bin/env python3
"""
Process judgment JSON files and convert to structured format

This script:
1. Scans all extracted judgment JSON files
2. Filters criminal cases (JID contains 'M')
3. Extracts basic metadata
4. Exports to CSV and Parquet formats
"""

import json
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import argparse
from multiprocessing import Pool, cpu_count
import time


# Paths
RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw" / "monthly_packages"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"


def parse_jid(jid: str) -> Dict[str, str]:
    """Parse JID components

    JID format: CourtCode + CaseType, Year, CaseCategory, Number, Date, CheckCode
    Example: SJEM,104,重秩聲,17,20160126,1
    """
    parts = jid.split(',')

    if len(parts) < 6:
        return {}

    court_case = parts[0]

    # Extract court code and case type
    # Case type: V=民事, M=刑事, A=行政, P=懲戒, C=憲法
    case_type = None
    court_code = None

    for i, char in enumerate(court_case):
        if char in ['V', 'M', 'A', 'P', 'C']:
            court_code = court_case[:i]
            case_type = char
            break

    return {
        'jid': jid,
        'court_code': court_code or court_case,
        'case_type': case_type,
        'year': parts[1],
        'case_category': parts[2],
        'case_number': parts[3],
        'judgment_date': parts[4],
        'check_code': parts[5] if len(parts) > 5 else None
    }


def is_criminal_case(jid: str) -> bool:
    """Check if this is a criminal case (contains 'M')"""
    return 'M' in jid.split(',')[0]


def extract_text_length(jfull: str) -> int:
    """Calculate text length (characters)"""
    return len(jfull) if jfull else 0


def process_json_file(json_path: Path) -> Optional[Dict]:
    """Process a single judgment JSON file"""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        jid = data.get('JID', '')

        # Skip if not criminal case
        if not is_criminal_case(jid):
            return None

        # Parse JID
        jid_parts = parse_jid(jid)

        # Extract basic fields
        record = {
            'jid': jid,
            'court_code': jid_parts.get('court_code', ''),
            'case_type': jid_parts.get('case_type', ''),
            'year': data.get('JYEAR', ''),
            'case_category': data.get('JCASE', ''),
            'case_number': data.get('JNO', ''),
            'judgment_date': data.get('JDATE', ''),
            'title': data.get('JTITLE', ''),
            'full_text': data.get('JFULL', ''),
            'text_length': extract_text_length(data.get('JFULL', '')),
            'source_file': str(json_path.relative_to(RAW_DATA_DIR)),
        }

        return record

    except Exception as e:
        return None


def process_batch(batch: List[Path]) -> Tuple[List[Dict], int]:
    """Process a batch of JSON files

    Args:
        batch: List of JSON file paths

    Returns:
        Tuple[List[Dict], int]: (records, error_count)
    """
    records = []
    error_count = 0

    for json_path in batch:
        record = process_json_file(json_path)
        if record:
            records.append(record)
        else:
            error_count += 1

    return records, error_count


def filter_files_by_date_range(json_files: List[Path], start_month: Optional[str], end_month: Optional[str]) -> List[Path]:
    """Filter JSON files by date range based on source directory

    Args:
        json_files: List of JSON file paths
        start_month: Start month in YYYYMM format (e.g., "199601")
        end_month: End month in YYYYMM format (e.g., "202607")

    Returns:
        Filtered list of JSON files
    """
    if not start_month and not end_month:
        return json_files

    filtered = []
    for json_path in json_files:
        # Extract YYYYMM from path like: monthly_packages/199601/199601/...
        parts = json_path.parts
        for part in parts:
            if len(part) == 6 and part.isdigit():
                file_month = part
                if start_month and file_month < start_month:
                    break
                if end_month and file_month > end_month:
                    break
                filtered.append(json_path)
                break

    return filtered


def scan_json_files_fast(start_month: Optional[str] = None, end_month: Optional[str] = None) -> List[Path]:
    """Fast scan of JSON files using direct directory traversal

    Args:
        start_month: Start month in YYYYMM format
        end_month: End month in YYYYMM format

    Returns:
        List of JSON file paths
    """
    import os

    print(f"Scanning directory: {RAW_DATA_DIR}")

    # If date range specified, only scan specific month directories
    if start_month or end_month:
        json_files = []
        # Get all month directories
        month_dirs = [d for d in RAW_DATA_DIR.iterdir() if d.is_dir() and len(d.name) == 6 and d.name.isdigit()]

        for month_dir in sorted(month_dirs):
            month = month_dir.name
            if start_month and month < start_month:
                continue
            if end_month and month > end_month:
                continue

            # Scan this month's directory
            for root, dirs, files in os.walk(month_dir):
                for file in files:
                    if file.endswith('.json'):
                        json_files.append(Path(root) / file)

        print(f"Found {len(json_files):,} JSON files in range {start_month or 'start'} - {end_month or 'end'}")
    else:
        # Scan all
        json_files = []
        for root, dirs, files in os.walk(RAW_DATA_DIR):
            for file in files:
                if file.endswith('.json'):
                    json_files.append(Path(root) / file)

        print(f"Found {len(json_files):,} JSON files")

    return json_files


def process_all_judgments_streaming(
    csv_file: Path,
    parquet_file: Path,
    limit: Optional[int] = None,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
    workers: Optional[int] = None,
    batch_size: int = 1000,
    export_csv: bool = True,
    export_parquet: bool = True
) -> Tuple[int, int]:
    """Process all judgment files with streaming (low memory usage)

    Args:
        csv_file: Output CSV file path
        parquet_file: Output Parquet file path
        limit: Maximum number of files to process (for testing)
        start_month: Start month in YYYYMM format (e.g., "199601")
        end_month: End month in YYYYMM format (e.g., "202607")
        workers: Number of worker processes (defaults to CPU count)
        batch_size: Number of files per batch (default: 1000)
        export_csv: Export to CSV
        export_parquet: Export to Parquet

    Returns:
        Tuple[int, int]: (criminal_count, total_files)
    """
    print(f"\n{'='*60}")
    print(f"Processing Judgment Files (Streaming Mode)")
    print(f"{'='*60}\n")

    # Fast scan
    json_files = scan_json_files_fast(start_month, end_month)

    if limit:
        json_files = json_files[:limit]
        print(f"Limiting to first {limit:,} files (test mode)")

    # Determine number of workers
    if workers is None:
        workers = cpu_count()

    print(f"Workers: {workers} (CPU cores: {cpu_count()})")
    print(f"Batch size: {batch_size}")
    print(f"\nProcessing with streaming (low memory usage)...\n")

    # Create batches
    batches = []
    for i in range(0, len(json_files), batch_size):
        batches.append(json_files[i:i + batch_size])

    print(f"Created {len(batches)} batches of ~{batch_size} files each\n")

    # Prepare output files
    csv_writer = None
    csv_f = None
    parquet_writer = None

    if export_csv:
        import csv as csv_module
        csv_f = open(csv_file, 'w', newline='', encoding='utf-8')
        # Will write header after first batch

    # Process batches with streaming
    start_time = time.time()
    criminal_count = 0
    processed_count = 0
    first_batch = True

    try:
        with Pool(processes=workers) as pool:
            for batch_idx, (batch_records, batch_errors) in enumerate(pool.imap(process_batch, batches)):
                criminal_count += len(batch_records)
                processed_count += len(batch_records) + batch_errors

                # Stream to CSV
                if export_csv and batch_records:
                    if first_batch:
                        # Write header
                        fieldnames = [k for k in batch_records[0].keys() if k != 'full_text']
                        csv_writer = csv_module.DictWriter(csv_f, fieldnames=fieldnames)
                        csv_writer.writeheader()
                        first_batch = False

                    # Write rows (without full_text)
                    for record in batch_records:
                        row = {k: v for k, v in record.items() if k != 'full_text'}
                        csv_writer.writerow(row)

                # Stream to Parquet (write batch file)
                if export_parquet and batch_records:
                    try:
                        import pandas as pd
                        df_batch = pd.DataFrame(batch_records)

                        # Write each batch to a separate file
                        batch_parquet = parquet_file.parent / f"{parquet_file.stem}_batch{batch_idx:05d}.parquet"
                        df_batch.to_parquet(batch_parquet, engine='pyarrow', index=False, compression='snappy')
                    except Exception as e:
                        print(f"\n⚠ Parquet write warning: {e}")

                # Progress update
                if (batch_idx + 1) % 10 == 0:  # Update every 10 batches
                    elapsed = time.time() - start_time
                    speed = processed_count / elapsed if elapsed > 0 else 0
                    print(f"  Processed {processed_count:,}/{len(json_files):,} files "
                          f"(Criminal: {criminal_count:,}) [{speed:.0f} files/sec]")

    finally:
        if csv_f:
            csv_f.close()

    elapsed = time.time() - start_time

    # Merge Parquet batch files into one
    if export_parquet:
        print(f"\nMerging Parquet batch files...")
        batch_files = sorted(parquet_file.parent.glob(f"{parquet_file.stem}_batch*.parquet"))

        if batch_files:
            try:
                import pandas as pd
                import pyarrow.parquet as pq
                import pyarrow as pa

                # Read all batch files and write to single file
                writer = None
                for batch_file in batch_files:
                    table = pq.read_table(batch_file)

                    if writer is None:
                        writer = pq.ParquetWriter(parquet_file, table.schema, compression='snappy')

                    writer.write_table(table)

                if writer:
                    writer.close()

                # Clean up batch files
                for batch_file in batch_files:
                    batch_file.unlink()

                print(f"✓ Merged {len(batch_files)} batch files into {parquet_file.name}")
            except Exception as e:
                print(f"⚠ Parquet merge warning: {e}")
                print(f"  Batch files kept at: {parquet_file.parent}")

    print(f"\n{'='*60}")
    print(f"Processing Complete")
    print(f"{'='*60}")
    print(f"Total files scanned: {len(json_files):,}")
    print(f"Criminal cases found: {criminal_count:,}")
    print(f"Non-criminal/errors: {len(json_files) - criminal_count:,}")
    print(f"Processing time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    print(f"Average speed: {len(json_files)/elapsed:.1f} files/second")
    print(f"{'='*60}\n")

    return criminal_count, len(json_files)


def export_to_csv(records: List[Dict], output_file: Path):
    """Export records to CSV (without full text for easier viewing)"""
    import csv

    print(f"Exporting to CSV: {output_file}")

    # Create summary without full text
    summary_records = []
    for r in records:
        summary = {k: v for k, v in r.items() if k != 'full_text'}
        summary_records.append(summary)

    if not summary_records:
        print("  No records to export")
        return

    fieldnames = summary_records[0].keys()

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_records)

    print(f"  ✓ Exported {len(summary_records):,} records")


def export_to_parquet(records: List[Dict], output_file: Path):
    """Export records to Parquet (with full text)"""
    try:
        import pandas as pd

        print(f"Exporting to Parquet: {output_file}")

        if not records:
            print("  No records to export")
            return

        df = pd.DataFrame(records)

        output_file.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(output_file, index=False, compression='snappy')

        print(f"  ✓ Exported {len(records):,} records")
        print(f"  File size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")

    except ImportError:
        print("  ⚠️  pandas not installed, skipping Parquet export")
        print("  Install with: pip install pandas pyarrow")


def main():
    parser = argparse.ArgumentParser(
        description='Process judgment JSON files and export to structured format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s                                        # Process all files
  %(prog)s --start-month 2020.01 --end-month 2020.12   # Process year 2020
  %(prog)s --start-month 199601 --end-month 199612     # Process 1996
  %(prog)s --limit 1000                           # Test with first 1000 files
  %(prog)s --force                                # Force reprocess (skip existing check)
  %(prog)s --csv-only                             # Only export CSV (skip Parquet)
        '''
    )

    parser.add_argument('--start-month', dest='start_month', help='Start month (YYYY.MM, YYYY-MM, or YYYYMM)')
    parser.add_argument('--end-month', dest='end_month', help='End month (YYYY.MM, YYYY-MM, or YYYYMM)')
    parser.add_argument('--limit', type=int, help='Limit number of files to process (for testing)')
    parser.add_argument('--force', '-f', action='store_true', help='Force reprocess existing files')
    parser.add_argument('--csv-only', action='store_true', help='Only export CSV (skip Parquet)')
    parser.add_argument('--parquet-only', action='store_true', help='Only export Parquet (skip CSV)')
    parser.add_argument('--workers', '-w', type=int, help=f'Number of worker processes (default: {cpu_count()} CPU cores)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Number of files per batch (default: 1000)')

    args = parser.parse_args()

    # Parse date range
    start_month = None
    end_month = None

    if args.start_month:
        # Convert YYYY.MM or YYYY-MM to YYYYMM
        start_month = args.start_month.replace('.', '').replace('-', '')
        if len(start_month) != 6 or not start_month.isdigit():
            print(f"✗ Invalid start month format: {args.start_month}")
            print("  Use YYYY.MM, YYYY-MM, or YYYYMM")
            return

    if args.end_month:
        end_month = args.end_month.replace('.', '').replace('-', '')
        if len(end_month) != 6 or not end_month.isdigit():
            print(f"✗ Invalid end month format: {args.end_month}")
            print("  Use YYYY.MM, YYYY-MM, or YYYYMM")
            return

    # Generate output filenames
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    base_name = f"criminal_judgments"

    if start_month or end_month:
        range_str = f"{start_month or 'start'}_{end_month or 'end'}"
        base_name += f"_{range_str}"
    else:
        base_name += "_all"

    base_name += f"_{timestamp}"

    if args.limit:
        base_name += f"_sample{args.limit}"

    # Check if output files exist (unless --force)
    csv_file = PROCESSED_DIR / f"{base_name}_summary.csv"
    parquet_file = PROCESSED_DIR / f"{base_name}.parquet"

    if not args.force:
        # Check for any files matching the date range pattern
        if start_month or end_month:
            pattern = f"criminal_judgments_{start_month or 'start'}_{end_month or 'end'}_*"
            existing = list(PROCESSED_DIR.glob(pattern + ".parquet"))
            if existing:
                print(f"\n✓ Found existing processed files for this range:")
                for f in existing:
                    print(f"  - {f.name}")
                print(f"\nSkipping processing. Use --force to reprocess.")
                return

    # Process all judgments with streaming (low memory)
    criminal_count, total_files = process_all_judgments_streaming(
        csv_file=csv_file,
        parquet_file=parquet_file,
        limit=args.limit,
        start_month=start_month,
        end_month=end_month,
        workers=args.workers,
        batch_size=args.batch_size,
        export_csv=not args.parquet_only,
        export_parquet=not args.csv_only
    )

    if criminal_count == 0:
        print("No criminal cases found!")
        return

    print(f"\n✓ Processing complete!")
    print(f"\nOutput files:")
    if not args.parquet_only and csv_file.exists():
        print(f"  - CSV: {csv_file} ({csv_file.stat().st_size / 1024 / 1024:.1f} MB)")
    if not args.csv_only and parquet_file.exists():
        print(f"  - Parquet: {parquet_file} ({parquet_file.stat().st_size / 1024 / 1024:.1f} MB)")
    print(f"\nOutput directory: {PROCESSED_DIR}")


if __name__ == "__main__":
    main()
