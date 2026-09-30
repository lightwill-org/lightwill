#!/usr/bin/env python3
"""
Analyze data quality of downloaded judgment files

Checks:
- JSON structure consistency
- JFULL content quality (vs "詳如附件")
- Criminal vs Civil case ratio
- File completeness
"""

import json
from pathlib import Path
from collections import defaultdict


def analyze_json_file(file_path: Path) -> dict:
    """Analyze a single JSON file"""
    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)

    result = {
        "has_jfull": bool(data.get("JFULL")),
        "jfull_length": len(data.get("JFULL", "")),
        "is_attachment_only": "詳如附件" in data.get("JFULL", ""),
        "has_pdf": bool(data.get("JPDF")),
        "case_type": "criminal" if "刑事" in str(file_path) else "civil",
        "jtitle": data.get("JTITLE", ""),
        "jyear": data.get("JYEAR", ""),
    }

    return result


def analyze_month(month_dir: Path) -> dict:
    """Analyze all JSON files in a month directory"""

    if not month_dir.exists():
        return {"error": f"Directory not found: {month_dir}"}

    # Find the nested directory structure (e.g., 199601/199601/)
    subdirs = list(month_dir.glob("*/"))
    if subdirs:
        month_dir = subdirs[0]

    json_files = list(month_dir.rglob("*.json"))

    if not json_files:
        return {"error": f"No JSON files found in {month_dir}"}

    stats = {
        "total_files": len(json_files),
        "criminal_files": 0,
        "civil_files": 0,
        "has_full_text": 0,
        "attachment_only": 0,
        "has_pdf": 0,
        "jfull_lengths": [],
        "case_titles": defaultdict(int),
    }

    for file_path in json_files:
        try:
            result = analyze_json_file(file_path)

            if result["case_type"] == "criminal":
                stats["criminal_files"] += 1
            else:
                stats["civil_files"] += 1

            if result["has_jfull"] and result["jfull_length"] > 100:
                stats["has_full_text"] += 1
                stats["jfull_lengths"].append(result["jfull_length"])

            if result["is_attachment_only"]:
                stats["attachment_only"] += 1

            if result["has_pdf"]:
                stats["has_pdf"] += 1

            if result["jtitle"]:
                stats["case_titles"][result["jtitle"]] += 1

        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    # Calculate averages
    if stats["jfull_lengths"]:
        stats["avg_jfull_length"] = sum(stats["jfull_lengths"]) / len(
            stats["jfull_lengths"]
        )
        stats["min_jfull_length"] = min(stats["jfull_lengths"])
        stats["max_jfull_length"] = max(stats["jfull_lengths"])

    # Top case titles
    stats["top_case_titles"] = dict(
        sorted(stats["case_titles"].items(), key=lambda x: x[1], reverse=True)[:10]
    )

    # Remove raw lengths to reduce output
    del stats["jfull_lengths"]
    del stats["case_titles"]

    return stats


def main():
    """Analyze all extracted month directories"""

    base_dir = Path(__file__).parent.parent / "data" / "raw" / "monthly_packages"

    # Find all month directories
    month_dirs = sorted([d for d in base_dir.glob("*") if d.is_dir()])

    if not month_dirs:
        print("✗ No extracted month directories found")
        print(f"  Expected location: {base_dir}")
        return

    print(f"\n{'=' * 80}")
    print("Data Quality Analysis")
    print(f"{'=' * 80}")
    print(f"Analyzing {len(month_dirs)} month(s)...\n")

    results = {}

    for month_dir in month_dirs:
        month_name = month_dir.name
        print(f"📊 Analyzing {month_name}...", end=" ")

        stats = analyze_month(month_dir)

        if "error" in stats:
            print(f"✗ {stats['error']}")
            continue

        results[month_name] = stats
        print(f"✓ ({stats['total_files']} files)")

    # Summary report
    print(f"\n{'=' * 80}")
    print("Summary Report")
    print(f"{'=' * 80}\n")

    for month_name, stats in results.items():
        print(f"{'─' * 80}")
        print(f"📅 {month_name}")
        print(f"{'─' * 80}")
        print(f"  Total files: {stats['total_files']}")
        print(
            f"  Criminal: {stats['criminal_files']} ({stats['criminal_files'] / stats['total_files'] * 100:.1f}%)"
        )
        print(
            f"  Civil: {stats['civil_files']} ({stats['civil_files'] / stats['total_files'] * 100:.1f}%)"
        )
        print(
            f"  Has full text: {stats['has_full_text']} ({stats['has_full_text'] / stats['total_files'] * 100:.1f}%)"
        )
        print(
            f"  Attachment only: {stats['attachment_only']} ({stats['attachment_only'] / stats['total_files'] * 100:.1f}%)"
        )
        print(
            f"  Has PDF: {stats['has_pdf']} ({stats['has_pdf'] / stats['total_files'] * 100:.1f}%)"
        )

        if "avg_jfull_length" in stats:
            print("\n  Full text length:")
            print(f"    Average: {stats['avg_jfull_length']:.0f} chars")
            print(f"    Min: {stats['min_jfull_length']} chars")
            print(f"    Max: {stats['max_jfull_length']} chars")

        print("\n  Top 5 case types:")
        for title, count in list(stats["top_case_titles"].items())[:5]:
            print(f"    {title}: {count}")

        print()

    # Overall statistics
    if results:
        print(f"{'=' * 80}")
        print("Overall Statistics")
        print(f"{'=' * 80}\n")

        total_files = sum(s["total_files"] for s in results.values())
        total_criminal = sum(s["criminal_files"] for s in results.values())
        total_full_text = sum(s["has_full_text"] for s in results.values())
        total_attachment = sum(s["attachment_only"] for s in results.values())

        print(f"  Total files analyzed: {total_files}")
        print(
            f"  Criminal cases: {total_criminal} ({total_criminal / total_files * 100:.1f}%)"
        )
        print(
            f"  With full text: {total_full_text} ({total_full_text / total_files * 100:.1f}%)"
        )
        print(
            f"  Attachment only: {total_attachment} ({total_attachment / total_files * 100:.1f}%)"
        )
        print()


if __name__ == "__main__":
    main()
