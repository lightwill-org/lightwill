"""
Stage 1 v2: Animal Protection Act documents by cited statutes OR title.

- statutes: cites_animal_protection_act from data/structured (criminal articles of the act
  in the statutes appendix, 主文 or reasons; see judgment_parser.py)
- title:    the original Stage 1 title match
Wildlife Act documents (title contains 野生動物) are dropped: poaching and wildlife trade
are out of scope. Non-criminal documents (JID court code 4th character != M) are dropped.

Each row is a document, not yet a case: appeal levels of the same case and procedural
rulings are kept and labelled so they can be merged later (unit of analysis step).

Output: data/filtered/stage1_v2.csv (no names)

Usage:
    python scripts/build_stage1_v2.py
"""

import glob
from pathlib import Path

import pandas as pd
from judgment_parser import ANIMAL_ACT_CRIME_RE, norm
from tqdm import tqdm

STRUCTURED_DIR = "data/structured"
PARQUET_DIR = "data/processed"
STAGE1_V1_CSV = "data/filtered/criminal_judgments_tagged_20261002_103027.csv"
SUMMARY_CSV = glob.glob("data/processed/*_summary.csv")[0]
OUTPUT_FILE = Path("data/filtered/stage1_v2.csv")

COLS = [
    "jid",
    "status",
    "doc_kind",
    "cites_animal_protection_act",
    "main_guilty",
    "main_acquitted",
    "main_not_accepted",
    "main_exempted",
    "main_dismissed",
    "main_revoked",
    "facts_start",
    "attach_facts_start",
    "defendant_hashes",
    "n_defendants_masked",
    *[
        f"{s}_{p}"
        for s in ("main", "statutes", "reasons", "combined")
        for p in ("start", "end")
    ],
]
COURT_LEVELS = {"D": "district", "E": "summary_court", "H": "high", "S": "supreme"}


def document_role(row) -> str:
    """Rough role of the document in a case, for later merging and filtering."""
    if row["doc_kind"] == "ruling" or row["status"] == "no_main":
        return "ruling_or_other"
    if row["main_guilty"]:
        return "conviction"
    if row["main_acquitted"] or row["main_not_accepted"] or row["main_exempted"]:
        return "acquittal_or_dismissal"
    if row["main_dismissed"]:
        return "appeal_dismissed"
    return "other_judgment"


def animal_basis(docs: pd.DataFrame) -> pd.Series:
    """
    Where the act's criminal articles appear:
    - main:         in 主文, so the defendant was convicted (or sentenced) under the act
    - statutes:     in the statutes appendix, which lists the articles applied
    - reasons_only: only discussed in the reasons; often an acquitted or unproven charge
                    (e.g. "因認被告涉犯動物保護法第25條罪嫌…" then 不另為無罪), needs review
    - none:         title match only
    """
    basis = pd.Series("none", index=docs.index)
    for batch, group in tqdm(docs.groupby("batch"), desc="Locating citations"):
        texts = pd.read_parquet(f"{PARQUET_DIR}/{batch}", columns=["jid", "full_text"])
        texts = texts.set_index("jid")["full_text"]
        for idx, row in group.iterrows():
            text = texts.get(row["jid"])
            if not isinstance(text, str):
                continue

            def found(*sections: str) -> bool:
                part = "".join(
                    text[max(row[f"{s}_start"], 0) : max(row[f"{s}_end"], 0)]
                    for s in sections
                )
                return bool(ANIMAL_ACT_CRIME_RE.search(norm(part)))

            if found("main"):
                basis[idx] = "main"
            elif found("statutes"):
                basis[idx] = "statutes"
            elif found("reasons", "combined"):
                basis[idx] = "reasons_only"
    return basis


def main():
    v1 = pd.read_csv(
        STAGE1_V1_CSV, usecols=["jid", "animal_case_stage1"], low_memory=False
    )
    title_jids = set(v1.loc[v1["animal_case_stage1"].eq(True), "jid"])

    parts = []
    for f in tqdm(
        sorted(glob.glob(f"{STRUCTURED_DIR}/*.parquet")), desc="Reading structured"
    ):
        df = pd.read_parquet(f, columns=COLS)
        keep = df["cites_animal_protection_act"].fillna(False) | df["jid"].isin(
            title_jids
        )
        parts.append(df[keep].assign(batch=Path(f).name))
    docs = pd.concat(parts, ignore_index=True)
    docs["animal_basis"] = animal_basis(docs)

    titles = pd.read_csv(SUMMARY_CSV, usecols=["jid", "title"], dtype=str)
    titles = titles.drop_duplicates("jid").set_index("jid")["title"]
    docs["title"] = docs["jid"].map(titles)

    by_law = docs["cites_animal_protection_act"].fillna(False).astype(bool)
    by_title = docs["jid"].isin(title_jids)
    docs["source"] = "both"
    docs.loc[by_law & ~by_title, "source"] = "statutes_only"
    docs.loc[~by_law & by_title, "source"] = "title_only"

    code = docs["jid"].str.split(",").str[0]
    n_before = len(docs)
    wildlife = docs["title"].fillna("").str.contains("野生動物")
    non_criminal = code.str[3] != "M"
    docs = docs[~wildlife & ~non_criminal].copy()
    print(
        f"Documents: {n_before:,} → {len(docs):,} "
        f"(dropped wildlife {wildlife.sum():,}, non-criminal {non_criminal.sum():,})"
    )

    parts = docs["jid"].str.split(",")
    docs["court_level"] = parts.str[0].str[2].map(COURT_LEVELS).fillna("other")
    docs["case_key"] = parts.str[:4].str.join(",")
    docs["year"] = parts.str[4].str[:4]
    docs["role"] = docs.apply(document_role, axis=1)
    docs["has_facts"] = (
        docs[["facts_start", "combined_start", "attach_facts_start"]] >= 0
    ).any(axis=1)
    docs["n_defendants_named"] = docs["defendant_hashes"].map(
        lambda x: 0 if x is None else len(x)
    )

    print("\nBy source:")
    print(docs["source"].value_counts().to_string())
    print("\nSource × document role:")
    print(pd.crosstab(docs["source"], docs["role"], margins=True).to_string())
    print("\nCourt level × document role:")
    print(pd.crosstab(docs["court_level"], docs["role"], margins=True).to_string())

    print("\nDocument role × where the act's criminal articles appear:")
    print(pd.crosstab(docs["role"], docs["animal_basis"], margins=True).to_string())

    docs["animal_conviction"] = (docs["role"] == "conviction") & docs[
        "animal_basis"
    ].isin(["main", "statutes"])
    conv = docs[docs["animal_conviction"]]
    print(
        "\nBelow: convictions under the act (conviction with the act in 主文 or statutes appendix)"
    )
    print(
        f"\nConvictions: {len(conv):,} | with facts section: {conv['has_facts'].mean() * 100:.1f}% "
        f"| with named defendants: {(conv['n_defendants_named'] > 0).mean() * 100:.1f}%"
    )
    print("\nConvictions by year:")
    print(conv["year"].value_counts().sort_index().to_string())
    print(
        "\nTitles of statutes-only convictions (offences hidden behind other titles):"
    )
    print(
        conv.loc[conv["source"] == "statutes_only", "title"]
        .value_counts()
        .head(20)
        .to_string()
    )

    cols = [
        "jid",
        "case_key",
        "year",
        "court_level",
        "doc_kind",
        "role",
        "animal_basis",
        "animal_conviction",
        "source",
        "title",
        "has_facts",
        "n_defendants_named",
        "n_defendants_masked",
        "batch",
    ]
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    docs[cols].sort_values("jid").to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
