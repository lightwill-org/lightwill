"""
Check whether defendants can be linked across judgments.

Samples random Parquet batches and reports, by judgment year:
- whether the defendant name in the header is masked (甲○○) or a full name
- whether birth date or ID number appears in the header
- how often prior records are described, and how often they cite a case number
- how often the same full name appears in multiple judgments (collision indicator)

Usage:
    python scripts/check_identity_linkage.py [n_batches]
"""

import glob
import random
import re
import sys
from collections import Counter

import pandas as pd
from tqdm import tqdm

PARQUET_DIR = "data/processed"

DEFENDANT_RE = re.compile(r"被[\s　]*告[\s　]+([^\s　，,（(]{1,12})")
MASKED_RE = re.compile(r"[○◯〇Ｏ０0]{1,3}|^[甲乙丙丁戊己庚辛壬癸][○◯〇Ｏ]")
FULL_NAME_RE = re.compile(r"^[一-鿿]{2,4}$")
BIRTH_ID_RE = re.compile(r"\d+\s*年\s*\d+\s*月\s*\d+\s*日生|身分證|統一編號")
HEADER_END_RE = re.compile(r"主[\s　]*文")

NUM = r"[\d０-９一二三四五六七八九十百千零○〇]"
PRIOR_RE = re.compile(r"(?:前|曾)(?:於[^，。]{0,20})?因[^。]{0,40}?案件")
CASE_NO_RE = re.compile(NUM + r"+\s*年度[^\s，。、]{1,8}?字第" + NUM + r"+")


def analyze(text: str) -> dict:
    header_end = HEADER_END_RE.search(text[:1500])
    header = text[: header_end.start()] if header_end else text[:800]

    names = DEFENDANT_RE.findall(header)
    first = names[0] if names else None
    masked = bool(first and MASKED_RE.search(first))
    full_name = bool(first and not masked and FULL_NAME_RE.match(first))

    prior = PRIOR_RE.search(text)
    prior_case_no = bool(
        prior and CASE_NO_RE.search(text[prior.start() : prior.end() + 200])
    )

    return {
        "defendant_found": first is not None,
        "masked": masked,
        "full_name": full_name,
        "name": first if full_name else None,
        "birth_or_id": bool(BIRTH_ID_RE.search(header)),
        "has_prior": prior is not None,
        "prior_case_no": prior_case_no,
    }


def main():
    n_batches = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    files = sorted(glob.glob(f"{PARQUET_DIR}/*_batch*.parquet"))
    random.seed(0)
    sample = random.sample(files, min(n_batches, len(files)))

    rows = []
    for f in tqdm(sample, desc="Sampling"):
        df = pd.read_parquet(f, columns=["jid", "full_text"])
        df = df[df["full_text"].str[:200].str.contains("判決", na=False)]
        for jid, text in zip(df["jid"], df["full_text"]):
            rows.append({"jid": jid, "year": int(jid.split(",")[1]), **analyze(text)})

    res = pd.DataFrame(rows)
    print(f"\nJudgments sampled: {len(res):,} (from {len(sample)} batches)")

    res["period"] = (
        (res["year"] // 5 * 5).astype(str)
        + "-"
        + (res["year"] // 5 * 5 + 4).astype(str)
    )
    summary = res.groupby("period").agg(
        n=("jid", "size"),
        defendant_found=("defendant_found", "mean"),
        masked=("masked", "mean"),
        full_name=("full_name", "mean"),
        birth_or_id=("birth_or_id", "mean"),
        has_prior=("has_prior", "mean"),
    )
    prior_rows = res[res["has_prior"]]
    summary["prior_cites_case_no"] = prior_rows.groupby("period")[
        "prior_case_no"
    ].mean()

    pct_cols = [
        "defendant_found",
        "masked",
        "full_name",
        "birth_or_id",
        "has_prior",
        "prior_cites_case_no",
    ]
    summary[pct_cols] = (summary[pct_cols] * 100).round(1)
    print("\nBy ROC year period (%):")
    print(summary.to_string())

    by_year = res.groupby("year")["full_name"].mean().mul(100).round(0)
    print("\nFull-name rate by year (%):")
    print(by_year.to_string())

    names = Counter(res["name"].dropna())
    if names:
        repeated = sum(c for c in names.values() if c >= 2)
        total = sum(names.values())
        print(
            f"\nFull names in sample: {total:,} judgments, {len(names):,} distinct names"
        )
        print(
            f"Judgments whose name appears 2+ times in the sample: {repeated / total * 100:.1f}%"
        )
        print(
            "(includes appeals of the same person and different people sharing a name)"
        )
        print("Most frequent names:")
        for name, c in names.most_common(15):
            print(f"  {name}: {c}")


if __name__ == "__main__":
    main()
