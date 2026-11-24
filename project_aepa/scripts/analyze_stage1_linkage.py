"""
Part A: validate case-number links using defendant name hashes.
Part B: first look at Stage 1 (Animal Protection Act) judgments through those links.

Part A: for each resolved link (citing judgment → cited prior judgment) where both sides
have defendant names (mostly ROC 100+), check whether they share a name hash. A wrong link
landing on a judgment with the same defendant name is very unlikely, so the shared rate
estimates link precision. Limits: masked names before ~2011 cannot be checked; renamed
defendants show as false mismatches.

Part B, for each Stage 1 judgment:
- backward: prior judgments it cites (what the defendant did before)
- forward:  later documents that cite it as a prior (what the same person did later)

Output: data/analysis/stage1_prior_links.csv (no names; one row per link)

Usage:
    python scripts/analyze_stage1_linkage.py [memory_limit_gb]
"""

import glob
import sys
from pathlib import Path

import pandas as pd
from resource_monitor import start_memory_guard

INDEX_DIR = "data/index"
STAGE1_CSV = "data/filtered/criminal_judgments_tagged_20261002_103027.csv"
SUMMARY_CSV = glob.glob("data/processed/*_summary.csv")[0]
OUTPUT_FILE = Path("data/analysis/stage1_prior_links.csv")


def name_shared(links: pd.DataFrame, defendants: pd.DataFrame) -> pd.Series:
    """Per link: True if both sides share a name hash, False if both have names but none
    shared, NA if either side has no extracted name."""
    links = links[["jid", "cited_jid"]].reset_index(names="link_id")
    citing = links.merge(defendants, on="jid")
    shared = citing.merge(
        defendants.rename(columns={"jid": "cited_jid"}), on=["cited_jid", "name_hash"]
    )["link_id"].unique()

    has_citing = set(citing["link_id"])
    named_jids = set(defendants["jid"])
    result = pd.Series(pd.NA, index=links["link_id"], dtype="boolean")
    both = links["link_id"].isin(has_citing) & links["cited_jid"].isin(named_jids)
    result[links.loc[both, "link_id"]] = False
    result[shared] = True
    return result


def load_titles(jids: set[str]) -> pd.DataFrame:
    parts = []
    for chunk in pd.read_csv(
        SUMMARY_CSV,
        usecols=["jid", "title", "judgment_date"],
        chunksize=500_000,
        dtype=str,
    ):
        parts.append(chunk[chunk["jid"].isin(jids)])
    return pd.concat(parts).drop_duplicates("jid").set_index("jid")


def part_a(links: pd.DataFrame, defendants: pd.DataFrame):
    print("=" * 60)
    print("Part A: link validation by shared defendant name")
    print("=" * 60)
    links = links.copy()
    links["name_shared"] = name_shared(links, defendants).values

    checkable = links[links["name_shared"].notna()]
    print(
        f"Resolved links: {len(links):,} | checkable (names on both sides): {len(checkable):,}"
    )
    print(f"Overall shared-name rate: {checkable['name_shared'].mean() * 100:.1f}%")

    for col in ["match", "method"]:
        table = checkable.groupby(col, observed=True)["name_shared"].agg(
            ["size", "mean"]
        )
        table["mean"] = (table["mean"] * 100).round(1)
        print(f"\nBy {col}:")
        print(table.rename(columns={"size": "n", "mean": "shared_pct"}).to_string())


def part_b(links: pd.DataFrame, defendants: pd.DataFrame):
    print("\n" + "=" * 60)
    print("Part B: Stage 1 (Animal Protection Act) judgments")
    print("=" * 60)
    stage1 = pd.read_csv(
        STAGE1_CSV, usecols=["jid", "animal_case_stage1"], low_memory=False
    )
    stage1_jids = set(stage1.loc[stage1["animal_case_stage1"] == True, "jid"])  # noqa: E712
    print(f"Stage 1 judgments: {len(stage1_jids):,}")

    result = pd.concat(
        [
            links[links["jid"].isin(stage1_jids)].assign(direction="backward"),
            links[links["cited_jid"].isin(stage1_jids)].assign(direction="forward"),
        ],
        ignore_index=True,
    )
    result["name_shared"] = name_shared(result, defendants).values

    titles = load_titles(set(result["jid"]) | set(result["cited_jid"]))
    result["citing_title"] = result["jid"].map(titles["title"])
    result["citing_date"] = result["jid"].map(titles["judgment_date"])
    result["cited_title"] = result["cited_jid"].map(titles["title"])
    result["cited_date"] = result["cited_jid"].map(titles["judgment_date"])

    def report(df: pd.DataFrame, label: str, anchor: str, other_title: str):
        print(f"\n--- {label} ---")
        print(
            f"Links: {len(df):,} | Stage 1 judgments involved: {df[anchor].nunique():,}"
        )
        checkable = df["name_shared"].notna()
        if checkable.any():
            print(
                f"Name check: {df.loc[checkable, 'name_shared'].mean() * 100:.1f}% shared "
                f"({checkable.sum():,} checkable)"
            )
        print(f"Top {other_title}:")
        print(df[other_title].value_counts().head(20).to_string())

    report(
        result[result["direction"] == "backward"],
        "Backward: priors cited by Stage 1 judgments",
        "jid",
        "cited_title",
    )
    report(
        result[result["direction"] == "forward"],
        "Forward: later documents citing a Stage 1 judgment as a prior",
        "cited_jid",
        "citing_title",
    )

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "direction",
        "jid",
        "citing_title",
        "citing_date",
        "cited_jid",
        "cited_title",
        "cited_date",
        "match",
        "method",
        "name_shared",
    ]
    result[cols].to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved: {OUTPUT_FILE}")


def main():
    limit_gb = float(sys.argv[1]) if len(sys.argv) > 1 else 6.0
    start_memory_guard(limit_gb)
    print(f"Memory limit: {limit_gb:g} GB")

    links = pd.read_parquet(
        f"{INDEX_DIR}/prior_case_refs_resolved.parquet",
        columns=["jid", "cited_jid", "match", "method", "resolved"],
    )
    links = (
        links[links["resolved"]]
        .drop(columns="resolved")
        .drop_duplicates(["jid", "cited_jid"])
    )
    links = links.reset_index(drop=True)
    defendants = pd.read_parquet(
        f"{INDEX_DIR}/defendants.parquet", columns=["jid", "name_hash"]
    )
    defendants = defendants.drop_duplicates()

    part_a(links, defendants)
    part_b(links, defendants)


if __name__ == "__main__":
    main()
