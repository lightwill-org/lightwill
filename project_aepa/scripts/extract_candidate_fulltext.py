"""
Extract full text for Stage 1 + Stage 2 candidates into a single Parquet file.

Scans all Parquet batch files once (instead of once per case), so downstream
LLM verification can load every candidate's text from memory.

Usage:
    python scripts/extract_candidate_fulltext.py [tagged_csv] [output_parquet]
"""

import glob
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm

TAGGED_CSV = (
    sys.argv[1]
    if len(sys.argv) > 1
    else ("data/filtered/criminal_judgments_tagged_20261002_103027_final_v2.csv")
)
OUTPUT_FILE = (
    sys.argv[2] if len(sys.argv) > 2 else "data/filtered/candidates_fulltext_v2.parquet"
)
PARQUET_DIR = "data/processed"


def load_candidates(tagged_csv: str) -> pd.DataFrame:
    cols = [
        "jid",
        "court_code",
        "year",
        "title",
        "animal_case_stage1",
        "animal_case_stage2",
    ]
    parts = []
    for chunk in pd.read_csv(
        tagged_csv, usecols=cols, chunksize=500_000, low_memory=False
    ):
        mask = (chunk["animal_case_stage1"].eq(True)) | (
            chunk["animal_case_stage2"].eq(True)
        )
        if mask.any():
            parts.append(chunk[mask])
    df = pd.concat(parts, ignore_index=True)
    return df.drop_duplicates(subset="jid")


def main():
    start = time.time()

    candidates = load_candidates(TAGGED_CSV)
    jids = set(candidates["jid"])
    print(f"Candidates: {len(jids):,} unique JIDs")

    parquet_files = sorted(glob.glob(f"{PARQUET_DIR}/*_batch*.parquet"))
    print(f"Scanning {len(parquet_files):,} Parquet files...")

    found = []
    for pf in tqdm(parquet_files, desc="Scanning"):
        # Read jid first; only load full_text when the batch has a match
        batch_jids = pq.read_table(pf, columns=["jid"]).column("jid").to_pylist()
        if jids.isdisjoint(batch_jids):
            continue
        df = pd.read_parquet(pf, columns=["jid", "full_text"])
        found.append(df[df["jid"].isin(jids)])

    texts = pd.concat(found, ignore_index=True).drop_duplicates(subset="jid")
    result = candidates.merge(texts, on="jid", how="left")

    missing = result["full_text"].isna().sum()
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(OUTPUT_FILE, index=False)

    size_mb = Path(OUTPUT_FILE).stat().st_size / 1024 / 1024
    print(f"\nSaved {len(result):,} cases to {OUTPUT_FILE} ({size_mb:.0f} MB)")
    print(f"Missing full text: {missing:,}")
    print(f"Elapsed: {(time.time() - start) / 60:.1f} min")


if __name__ == "__main__":
    main()
