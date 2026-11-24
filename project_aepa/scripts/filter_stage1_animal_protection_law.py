"""
Stage 1: Tag Animal Protection Law Cases

Adds filter tags to processed data to identify animal protection law cases.
This is the "Animal-first" approach focusing on cases with clear legal categorization.

Strategy:
- Read processed CSV (all 7.73M cases)
- Add filter columns: animal_case_stage1, matched_keywords_stage1
- Keep all records, just add tags

Output: data/filtered/criminal_judgments_tagged.csv (all 7.73M cases with tags)
"""

import glob
from datetime import datetime
from pathlib import Path

import pandas as pd


def tag_stage1_animal_cases(
    csv_file: str, output_dir: str = "data/filtered"
) -> pd.DataFrame:
    """Add stage 1 filter tags to all cases"""

    print("=" * 60)
    print("Stage 1: Tag Animal Protection Law Cases")
    print("=" * 60)

    # Define keywords for animal protection cases
    # Include: Animal Protection Act
    # Exclude: wildlife (mostly hunting/trading), disease control, drug management
    include_keywords = [
        "動物保護法",  # Animal Protection Act
    ]

    exclude_keywords = [
        "野生動物保育法",  # Wildlife Conservation Act (mostly hunting, not abuse)
        "動物傳染病",  # Animal disease control
        "動物用藥",  # Animal drug management
    ]

    print(f"\nInclude keywords: {include_keywords}")
    print(f"Exclude keywords: {exclude_keywords}")
    print(f"\nReading: {csv_file}")

    # Process in chunks and write to new file
    chunk_size = 100000
    total_processed = 0
    total_matched = 0
    first_chunk = True

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_path / f"criminal_judgments_tagged_{timestamp}.csv"

    print(f"\nProcessing and writing to: {output_file}")

    for chunk in pd.read_csv(csv_file, chunksize=chunk_size, low_memory=False):
        total_processed += len(chunk)

        # Add filter columns
        chunk["animal_case_stage1"] = False
        chunk["matched_keywords_stage1"] = ""

        # Filter: include keywords
        mask = chunk["title"].str.contains(
            "|".join(include_keywords), na=False, case=False
        )

        # Filter: exclude keywords
        for exclude_kw in exclude_keywords:
            mask = mask & ~chunk["title"].str.contains(exclude_kw, na=False, case=False)

        # Tag matched cases
        chunk.loc[mask, "animal_case_stage1"] = True
        chunk.loc[mask, "matched_keywords_stage1"] = chunk.loc[mask, "title"].apply(
            lambda x: ", ".join([kw for kw in include_keywords if kw in str(x)])
        )

        matched_count = mask.sum()
        total_matched += matched_count

        # Write to CSV (append mode after first chunk)
        if first_chunk:
            chunk.to_csv(output_file, index=False, encoding="utf-8", mode="w")
            first_chunk = False
        else:
            chunk.to_csv(
                output_file, index=False, encoding="utf-8", mode="a", header=False
            )

        if total_processed % 500000 == 0:
            print(
                f"  Processed {total_processed:,} records (matched: {total_matched:,})..."
            )

    print(f"\nTotal processed: {total_processed:,}")
    print(f"Total matched: {total_matched:,}")
    print(f"Match rate: {total_matched / total_processed * 100:.2f}%")

    print(f"\n✓ Saved to: {output_file}")

    return output_file, total_matched


def analyze_tagged_data(tagged_file: str):
    """Analyze the tagged data"""

    print("\n" + "=" * 60)
    print("Analysis of Tagged Data")
    print("=" * 60)

    print(f"\nReading tagged file: {tagged_file}")

    # Read only matched cases for analysis
    matched_cases = []
    chunk_size = 100000

    for chunk in pd.read_csv(tagged_file, chunksize=chunk_size, low_memory=False):
        matched = chunk[chunk["animal_case_stage1"].eq(True)]
        if len(matched) > 0:
            matched_cases.append(matched)

    if not matched_cases:
        print("⚠️  No matched cases found in tagged file")
        return

    df_matched = pd.concat(matched_cases, ignore_index=True)

    print(f"\n📊 Total matched cases: {len(df_matched):,}")

    # Year distribution
    print("\n📅 Year distribution (Top 10):")
    year_dist = df_matched["year"].value_counts().sort_index(ascending=False).head(10)
    for year, count in year_dist.items():
        print(f"  {year}: {count:,} cases")

    # Court distribution
    print("\n🏛️  Court distribution (Top 10):")
    court_dist = df_matched["court_code"].value_counts().head(10)
    for court, count in court_dist.items():
        print(f"  {court}: {count:,} cases")

    # Case category distribution
    print("\n📋 Case category distribution (Top 10):")
    category_dist = df_matched["case_category"].value_counts().head(10)
    for category, count in category_dist.items():
        print(f"  {category}: {count:,} cases")

    # Title distribution
    print("\n📝 Case title distribution:")
    title_dist = df_matched["title"].value_counts()
    for title, count in title_dist.items():
        print(f"  {title}: {count:,} cases")

    # Sample cases
    print("\n📄 Sample cases (first 5):")
    print(
        df_matched[["jid", "title", "year", "judgment_date"]]
        .head(5)
        .to_string(index=False)
    )

    # How to use the tagged data
    print("\n" + "=" * 60)
    print("How to Use the Tagged Data")
    print("=" * 60)
    print(
        """
# Load the tagged data
df = pd.read_csv('data/filtered/criminal_judgments_tagged_*.csv')

# Filter only matched cases
df_animal = df[df['animal_case_stage1'] == True]

# Or see all data with tags
print(df[['jid', 'title', 'animal_case_stage1', 'matched_keywords_stage1']].head())
"""
    )


if __name__ == "__main__":
    # Find the CSV file
    csv_files = glob.glob("data/processed/*_summary.csv")

    if not csv_files:
        print("❌ Error: No CSV file found in data/processed/")
        exit(1)

    csv_file = csv_files[0]

    # Execute tagging
    output_file, total_matched = tag_stage1_animal_cases(csv_file)

    if total_matched > 0:
        # Analyze results
        analyze_tagged_data(output_file)

        print("\n" + "=" * 60)
        print("✓ Stage 1 Complete")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Use the tagged CSV for analysis")
        print("2. Review matched cases manually")
        print("3. Decide if Stage 2 (fulltext search) is needed")
    else:
        print("\n❌ No cases found. Check keywords or data.")
