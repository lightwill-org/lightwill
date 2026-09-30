#!/usr/bin/env python3
"""
Extract RAR files from monthly packages

Cross-platform extraction using patool library.
Requires system-level RAR tool (unar, unrar, or 7-Zip) as backend.
"""

import argparse
import sys
from pathlib import Path
from multiprocessing import Pool, cpu_count

try:
    import patoolib
except ImportError:
    print("✗ Error: patool not installed")
    print("\nPlease install dependencies:")
    print("  pip install -r requirements.txt")
    print("\nNote: The package is 'patool' but imports as 'patoolib'")
    sys.exit(1)


def normalize_month(month_str: str) -> str:
    """Normalize month string to YYYYMM format

    Args:
        month_str: Month in format YYYY.MM, YYYY-MM, or YYYYMM

    Returns:
        str: Month in YYYYMM format

    Examples:
        normalize_month("2020.01") -> "202001"
        normalize_month("2020-01") -> "202001"
        normalize_month("202001") -> "202001"
    """
    month_str = month_str.replace(".", "").replace("-", "")
    if len(month_str) != 6 or not month_str.isdigit():
        raise ValueError(
            f"Invalid month format: {month_str}. Use YYYY.MM, YYYY-MM, or YYYYMM"
        )
    return month_str


def filter_rar_by_date_range(
    rar_files: list[Path], start_month: str | None, end_month: str | None
) -> list[Path]:
    """Filter RAR files by date range

    Args:
        rar_files: List of RAR file paths
        start_month: Start month in YYYYMM format (e.g., "199601")
        end_month: End month in YYYYMM format (e.g., "202607")

    Returns:
        List[Path]: Filtered list of RAR files
    """
    if not start_month and not end_month:
        return rar_files

    filtered = []
    for rar_path in rar_files:
        # Extract YYYYMM from filename (e.g., "199601.rar" -> "199601")
        stem = rar_path.stem
        if len(stem) == 6 and stem.isdigit():
            file_month = stem

            # Check if within range
            if start_month and file_month < start_month:
                continue
            if end_month and file_month > end_month:
                continue

            filtered.append(rar_path)

    return filtered


def extract_rar_worker(
    args: tuple[Path, Path | None, bool, bool, int, int],
) -> tuple[bool, str, str]:
    """Worker function for multiprocessing

    Args:
        args: Tuple of (rar_path, output_dir, verbose, force, idx, total)

    Returns:
        Tuple[bool, str, str]: (success, rar_name, message)
    """
    rar_path, output_dir, verbose, force, idx, total = args

    if output_dir is None:
        output_dir = rar_path.parent / rar_path.stem

    # Check if already extracted
    if not force and output_dir.exists():
        files = list(output_dir.iterdir())
        if files:
            return (
                True,
                rar_path.name,
                f"[{idx}/{total}] ⊘ Skipping (already extracted): {rar_path.name}",
            )

    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        patoolib.extract_archive(str(rar_path), outdir=str(output_dir), verbosity=-1)
        return True, rar_path.name, f"[{idx}/{total}] ✓ Extracted: {rar_path.name}"
    except Exception as e:
        return (
            False,
            rar_path.name,
            f"[{idx}/{total}] ✗ Extraction failed: {rar_path.name} - {e}",
        )


def extract_rar(
    rar_path: Path,
    output_dir: Path | None = None,
    verbose: bool = True,
    force: bool = False,
) -> bool:
    """Extract a RAR file using patool

    Args:
        rar_path: Path to RAR file
        output_dir: Output directory (defaults to same directory as RAR)
        verbose: Print progress messages
        force: Force re-extraction even if directory exists

    Returns:
        bool: True if extraction succeeded or was skipped, False if failed
    """

    if not rar_path.exists():
        print(f"✗ Error: File not found: {rar_path}")
        return False

    if output_dir is None:
        # Extract to same directory with same name
        output_dir = rar_path.parent / rar_path.stem

    # Check if already extracted
    if not force and output_dir.exists():
        # Check if directory has files
        files = list(output_dir.iterdir())
        if files:
            if verbose:
                print(f"⊘ Skipping (already extracted): {rar_path.name}")
            return True

    output_dir.mkdir(parents=True, exist_ok=True)

    if verbose:
        print(f"Extracting: {rar_path.name}")
        print(f"Output: {output_dir}")

    try:
        patoolib.extract_archive(str(rar_path), outdir=str(output_dir), verbosity=-1)
        if verbose:
            print("✓ Extracted successfully")
        return True
    except Exception as e:
        print(f"✗ Extraction failed: {e}")
        return False


def extract_all(
    source_dir: Path = None,
    output_dir: Path = None,
    pattern: str = "*.rar",
    force: bool = False,
    start_month: str | None = None,
    end_month: str | None = None,
    workers: int | None = None,
) -> tuple[int, int, int]:
    """Extract all RAR files in a directory with multiprocessing

    Args:
        source_dir: Source directory containing RAR files
        output_dir: Output directory (defaults to same as source)
        pattern: File pattern to match (default: *.rar)
        force: Force re-extraction even if directories exist
        start_month: Start month in YYYYMM format (e.g., "199601")
        end_month: End month in YYYYMM format (e.g., "202607")
        workers: Number of worker processes (defaults to CPU count)

    Returns:
        tuple: (success_count, skipped_count, total_count)
    """

    if source_dir is None:
        source_dir = Path(__file__).parent.parent / "data" / "raw" / "monthly_packages"

    if not source_dir.exists():
        print(f"✗ Error: Directory not found: {source_dir}")
        return 0, 0, 0

    rar_files = sorted(source_dir.glob(pattern))

    if not rar_files:
        print(f"✗ No RAR files found in: {source_dir}")
        return 0, 0, 0

    # Filter by date range
    rar_files = filter_rar_by_date_range(rar_files, start_month, end_month)

    if not rar_files:
        print("✗ No RAR files found in specified date range")
        return 0, 0, 0

    # Determine number of workers
    if workers is None:
        workers = cpu_count()

    print(f"\n{'=' * 60}")
    print("RAR Extraction")
    print(f"{'=' * 60}")
    print(f"Source: {source_dir}")
    print(f"Found: {len(rar_files)} RAR files")
    if start_month or end_month:
        range_str = f"{start_month or 'start'} - {end_month or 'end'}"
        print(f"Date range: {range_str}")
    if force:
        print("Mode: Force re-extraction")
    print(f"Workers: {workers} (CPU cores: {cpu_count()})")
    print(f"{'=' * 60}\n")

    # Prepare tasks
    tasks = []
    for idx, rar_file in enumerate(rar_files, 1):
        if output_dir:
            target_dir = output_dir / rar_file.stem
        else:
            target_dir = None
        tasks.append((rar_file, target_dir, False, force, idx, len(rar_files)))

    # Extract with multiprocessing
    import time

    start_time = time.time()

    success_count = 0
    skipped_count = 0
    failed_files = []

    with Pool(processes=workers) as pool:
        for success, rar_name, message in pool.imap_unordered(
            extract_rar_worker, tasks
        ):
            print(message)

            if success:
                if "Skipping" in message:
                    skipped_count += 1
                else:
                    success_count += 1
            else:
                failed_files.append(rar_name)

    elapsed = time.time() - start_time

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Extraction completed in {elapsed:.1f} seconds ({elapsed / 60:.1f} minutes)")
    print(f"Extracted: {success_count}/{len(rar_files)}")
    if skipped_count > 0:
        print(f"Skipped (already extracted): {skipped_count}/{len(rar_files)}")
    print(f"Average speed: {len(rar_files) / elapsed:.1f} files/second")

    if failed_files:
        print(f"\nFailed files ({len(failed_files)}):")
        for filename in failed_files:
            print(f"  - {filename}")
    else:
        print("\n✓ All files processed successfully!")

    print(f"{'=' * 60}\n")

    return success_count, skipped_count, len(rar_files)


def main():
    """Main entry point"""

    parser = argparse.ArgumentParser(
        description="Extract RAR files from monthly judgment packages",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract all RAR files (skip already extracted)
  python extract_rar.py

  # Extract specific time range
  python extract_rar.py --start-month 2020.01 --end-month 2020.12

  # Extract specific year
  python extract_rar.py --start-month 199601 --end-month 199612

  # Force re-extract all files
  python extract_rar.py --force

  # Force re-extract specific range
  python extract_rar.py --start-month 2020.01 --end-month 2020.12 --force

  # Extract single file
  python extract_rar.py data/raw/monthly_packages/199601.rar

  # Extract to specific directory
  python extract_rar.py data/raw/monthly_packages/199601.rar /path/to/output
        """,
    )

    parser.add_argument(
        "rar_file", nargs="?", help="Single RAR file to extract (optional)"
    )

    parser.add_argument(
        "output_dir", nargs="?", help="Output directory for single file extraction"
    )

    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force re-extraction even if directory already exists",
    )

    parser.add_argument(
        "--source-dir",
        type=Path,
        help="Source directory containing RAR files (default: data/raw/monthly_packages)",
    )

    parser.add_argument(
        "--start-month",
        dest="start_month",
        help='Start month (YYYY.MM, YYYY-MM, or YYYYMM format, e.g., "1996.01")',
    )

    parser.add_argument(
        "--end-month",
        dest="end_month",
        help='End month (YYYY.MM, YYYY-MM, or YYYYMM format, e.g., "2026.07")',
    )

    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        help=f"Number of worker processes (default: {cpu_count()} CPU cores)",
    )

    args = parser.parse_args()

    # Normalize month formats
    start_month = None
    end_month = None

    try:
        if args.start_month:
            start_month = normalize_month(args.start_month)
        if args.end_month:
            end_month = normalize_month(args.end_month)
    except ValueError as e:
        print(f"✗ Error: {e}")
        sys.exit(1)

    if args.rar_file:
        # Single file extraction
        rar_path = Path(args.rar_file)
        if not rar_path.is_absolute():
            rar_path = Path.cwd() / rar_path

        output_dir = Path(args.output_dir) if args.output_dir else None
        extract_rar(rar_path, output_dir, verbose=True, force=args.force)
    else:
        # Extract all files
        print("Extracting RAR files from monthly packages...")
        if not args.force:
            print(
                "(Already extracted files will be skipped. Use --force to re-extract.)\n"
            )
        extract_all(
            source_dir=args.source_dir,
            force=args.force,
            start_month=start_month,
            end_month=end_month,
            workers=args.workers,
        )


if __name__ == "__main__":
    main()
