"""
Resolve case numbers cited in prior-record text to JIDs in our dataset.

A JID starts with court_code,year,case_word,case_no (e.g. TYDM,104,易,587,...).
Court names are mapped to court codes using court_names.parquet, which pairs the court
name in each document header with the court code in its JID.

Court source (column `method`):
- named_court:   the citation names the court explicitly (or 同院 carries it forward)
- this_court:    the citation says 本院, so the citing document's court is used
- assumed_court: no court found; the citing document's court is assumed (lower confidence)

Matching (column `match`), tried in order:
- exact:           court code + case word as cited
- normalized_word: case word normalized (臺上→台上, 上更㈠→上更(一)/上更一)
- court_fallback:  summary courts have their own codes (士簡 → SLEM, not SLDM), so if the
                   cited court never uses this case word, try the courts that do; accepted
                   only when exactly one of them has the case

Memory: only cited keys are kept, and a watchdog aborts above the limit (default 6 GB).

Output: data/index/prior_case_refs_resolved.parquet

Usage:
    python scripts/resolve_prior_refs.py [memory_limit_gb]
"""

import glob
import re
import sys
from collections import defaultdict

import pandas as pd
import pyarrow.parquet as pq
from resource_monitor import start_memory_guard
from tqdm import tqdm

INDEX_DIR = "data/index"
PARQUET_DIR = "data/processed"
CATEGORY_COLS = [
    "cited_court",
    "cited_word",
    "ref_type",
    "citing_court",
    "cited_court_code",
    "method",
]

CIRCLED = dict(zip("㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩", "一二三四五六七八九十"))
RETRIAL_PLAIN_RE = re.compile(r"更([一二三四五六七八九十]+)")
RETRIAL_PAREN_RE = re.compile(r"更[（(]([一二三四五六七八九十]+)[)）]")


def normalize_court(name: str) -> str:
    return name.replace("台灣", "臺灣")


def jid_key(jid: str) -> str:
    return ",".join(jid.split(",")[:4])


def word_variants(word: str) -> list[str]:
    w = word.replace("臺", "台")
    for circled, cn in CIRCLED.items():
        w = w.replace(circled, f"({cn})")
    variants = [w]
    if RETRIAL_PAREN_RE.search(w):
        variants.append(RETRIAL_PAREN_RE.sub(r"更\1", w))
        variants.append(RETRIAL_PAREN_RE.sub(r"更（\1）", w))
    elif RETRIAL_PLAIN_RE.search(w):
        variants.append(RETRIAL_PLAIN_RE.sub(r"更(\1)", w))
    return list(dict.fromkeys(v for v in variants if v != word))


def scan_jids(desc: str, on_jid):
    for f in tqdm(sorted(glob.glob(f"{PARQUET_DIR}/*_batch*.parquet")), desc=desc):
        for jid in pq.read_table(f, columns=["jid"]).column("jid").to_pylist():
            on_jid(jid)


def load_word_codes() -> dict[str, set[str]]:
    word_codes: dict[str, set[str]] = defaultdict(set)

    def collect(jid: str):
        parts = jid.split(",")
        if len(parts) > 2:
            word_codes[parts[2]].add(parts[0])

    scan_jids("Collecting case words", collect)
    return word_codes


def candidate_keys(
    code: str, year: str, word: str, no: str, word_codes: dict[str, set[str]]
) -> tuple[list[tuple[str, str]], list[str]]:
    """Returns ordered (key, match) candidates, and fallback keys (accepted only if unique)."""
    primary = [(f"{code},{year},{word},{no}", "exact")]
    primary += [
        (f"{code},{year},{w},{no}", "normalized_word") for w in word_variants(word)
    ]

    fallback = []
    for w in [word, *word_variants(word)]:
        codes = word_codes.get(w, set())
        if code not in codes:
            fallback += [f"{c},{year},{w},{no}" for c in codes]
    return primary, fallback


def main():
    limit_gb = float(sys.argv[1]) if len(sys.argv) > 1 else 6.0
    start_memory_guard(limit_gb)
    print(f"Memory limit: {limit_gb:g} GB")

    refs = pd.read_parquet(f"{INDEX_DIR}/prior_case_refs.parquet")
    courts = pd.read_parquet(f"{INDEX_DIR}/court_names.parquet")
    courts["court_name"] = courts["court_name"].map(normalize_court)
    court_map = (
        courts.sort_values("n_docs", ascending=False)
        .drop_duplicates("court_name")
        .set_index("court_name")["court_code"]
    )
    print(f"Prior refs: {len(refs):,} | court names mapped: {len(court_map):,}")

    refs["citing_court"] = refs["jid"].str.split(",").str[0]
    refs["cited_court"] = refs["cited_court"].map(normalize_court)

    named = refs["cited_court"].map(court_map)
    is_this = refs["cited_court"] == "本院"
    refs["cited_court_code"] = named.where(~is_this, refs["citing_court"])
    refs["method"] = "none"
    refs.loc[named.notna(), "method"] = "named_court"
    refs.loc[is_this, "method"] = "this_court"

    no_court = (refs["ref_type"] == "court") & refs["cited_court_code"].isna()
    refs.loc[no_court, "cited_court_code"] = refs.loc[no_court, "citing_court"]
    refs.loc[no_court, "method"] = "assumed_court"

    usable = (
        (refs["ref_type"] == "court")
        & refs["cited_year"].ne("")
        & refs["cited_no"].ne("")
    )

    word_codes = load_word_codes()

    rows = refs.loc[
        usable, ["cited_court_code", "cited_year", "cited_word", "cited_no"]
    ]
    candidates = {}
    needed: set[str] = set()
    for idx, code, year, word, no in tqdm(
        rows.itertuples(), total=len(rows), desc="Building candidates"
    ):
        primary, fallback = candidate_keys(code, year, word, no, word_codes)
        candidates[idx] = (primary, fallback)
        needed.update(k for k, _ in primary)
        needed.update(fallback)
    print(f"Candidate keys: {len(needed):,}")

    found: dict[str, str] = {}

    def match(jid: str):
        key = jid_key(jid)
        if key in needed and key not in found:
            found[key] = jid

    scan_jids("Matching JIDs", match)
    needed.clear()  # free memory; nested match() still references it

    cited_jid: dict[int, str] = {}
    match_type: dict[int, str] = {}
    for idx, (primary, fallback) in candidates.items():
        for key, kind in primary:
            if key in found:
                cited_jid[idx], match_type[idx] = found[key], kind
                break
        else:
            hits = [found[k] for k in fallback if k in found]
            if len(hits) == 1:
                cited_jid[idx], match_type[idx] = hits[0], "court_fallback"
            elif len(hits) > 1:
                match_type[idx] = "ambiguous"
    del candidates

    refs["cited_jid"] = pd.Series(cited_jid, dtype="object").reindex(refs.index)
    refs["match"] = (
        pd.Series(match_type, dtype="object").reindex(refs.index).fillna("none")
    )
    refs["resolved"] = refs["cited_jid"].notna()
    for col in CATEGORY_COLS + ["match"]:
        refs[col] = refs[col].astype("category")

    out = f"{INDEX_DIR}/prior_case_refs_resolved.parquet"
    refs.to_parquet(out, index=False)

    print("\nReference type:")
    print(refs["ref_type"].value_counts().to_string())

    court_refs = refs[refs["ref_type"] == "court"]
    print("\nCourt references by court source (count, resolved %):")
    summary = court_refs.groupby("method", observed=True).agg(
        n=("resolved", "size"), resolved_pct=("resolved", "mean")
    )
    summary["resolved_pct"] = (summary["resolved_pct"] * 100).round(1)
    print(summary.to_string())

    print("\nHow resolved references were matched:")
    print(court_refs["match"].value_counts().to_string())

    confident = court_refs[court_refs["method"].isin(["named_court", "this_court"])]
    no_drug_ruling = confident[confident["cited_word"] != "毒聲"]
    print(
        f"\nResolved with named court or 本院: {confident['resolved'].sum():,} / {len(confident):,} "
        f"({confident['resolved'].mean() * 100:.1f}%)"
    )
    print(
        f"  excluding 毒聲 (mostly unpublished): {no_drug_ruling['resolved'].mean() * 100:.1f}%"
    )
    print(
        f"Documents with at least one resolved prior: {refs.loc[refs['resolved'], 'jid'].nunique():,}"
    )
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
