"""
Diagnose why cited prior case numbers fail to resolve to JIDs.

Checks, for citations with a named court or 本院:
1. Cited year before the dataset starts (ROC 85 = 1996): cannot exist in our data
2. Case word never appears in any JID: likely an unpublished type (e.g. juvenile)
   or an extraction error
3. Resolution rate by case word and by cited year

Usage:
    python scripts/diagnose_prior_refs.py
"""

import glob
from collections import Counter

import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm

INDEX_DIR = "data/index"
PARQUET_DIR = "data/processed"
DATASET_FIRST_YEAR = 85


def load_jid_words() -> Counter:
    words: Counter = Counter()
    for f in tqdm(
        sorted(glob.glob(f"{PARQUET_DIR}/*_batch*.parquet")),
        desc="Collecting case words",
    ):
        for jid in pq.read_table(f, columns=["jid"]).column("jid").to_pylist():
            parts = jid.split(",")
            if len(parts) > 2:
                words[parts[2]] += 1
    return words


def main():
    refs = pd.read_parquet(
        f"{INDEX_DIR}/prior_case_refs_resolved.parquet",
        columns=[
            "ref_type",
            "method",
            "cited_year",
            "cited_word",
            "cited_no",
            "resolved",
        ],
    )
    refs = refs[
        (refs["ref_type"] == "court")
        & refs["method"].isin(["named_court", "this_court"])
    ].copy()
    refs["cited_word"] = refs["cited_word"].astype(str)
    refs["year_num"] = pd.to_numeric(refs["cited_year"], errors="coerce")

    jid_words = load_jid_words()
    refs["word_in_dataset"] = refs["cited_word"].isin(set(jid_words))

    n = len(refs)
    unresolved = refs[~refs["resolved"]]
    print(
        f"Citations with named court or 本院: {n:,} | unresolved: {len(unresolved):,} "
        f"({len(unresolved) / n * 100:.1f}%)"
    )

    reasons = pd.Series("other", index=unresolved.index)
    reasons[unresolved["cited_no"].eq("") | unresolved["year_num"].isna()] = (
        "number_parse_failed"
    )
    reasons[unresolved["year_num"] < DATASET_FIRST_YEAR] = "before_dataset_(<ROC85)"
    reasons[~unresolved["word_in_dataset"] & reasons.eq("other")] = (
        "case_word_not_in_dataset"
    )
    print("\nUnresolved by reason:")
    counts = reasons.value_counts()
    print(
        pd.DataFrame(
            {"n": counts, "pct": (counts / len(unresolved) * 100).round(1)}
        ).to_string()
    )

    print("\nTop case words among unresolved (excluding pre-dataset years):")
    in_range = unresolved[unresolved["year_num"] >= DATASET_FIRST_YEAR]
    top = in_range["cited_word"].value_counts().head(25)
    by_word = refs.groupby("cited_word")["resolved"].agg(["size", "mean"])
    table = pd.DataFrame(
        {
            "unresolved": top,
            "total": by_word.loc[top.index, "size"],
            "resolved_pct": (by_word.loc[top.index, "mean"] * 100).round(1),
            "in_dataset_jids": [jid_words.get(w, 0) for w in top.index],
        }
    )
    print(table.to_string())

    print("\nResolution rate by cited year (ROC):")
    refs["period"] = (refs["year_num"] // 5 * 5).astype("Int64")
    by_year = refs.groupby("period")["resolved"].agg(["size", "mean"])
    by_year["mean"] = (by_year["mean"] * 100).round(1)
    print(by_year.rename(columns={"size": "n", "mean": "resolved_pct"}).to_string())

    odd = refs.loc[~refs["word_in_dataset"], "cited_word"].value_counts().head(15)
    print(
        "\nCase words never seen in any JID (possible extraction errors or unpublished types):"
    )
    print(odd.to_string())


if __name__ == "__main__":
    main()
