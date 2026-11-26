"""
Build the Stage 3 LLM input: candidates = Stage 1 v2 ∪ Stage 2 v3.

For each candidate document, the LLM reads passages around animal mentions, taken from
the same text Stage 2 searched (facts sections). Documents without a facts section
(rulings, attachments not published) fall back to the reasons, then to the full text;
`passage_source` records which was used.

Stage 1 documents go through the LLM too: harm type needs classifying, and convictions
under the Animal Protection Act are known positives that measure the LLM's recall.

Output: data/filtered/stage3_input.parquet (stage3_input_test.parquet with --test)
    jid, title, in_stage1, in_stage2, stage1_conviction, stage1_role, animal_basis,
    Stage 2 tags, core, passage_source, passages, passages_truncated

Usage:
    python scripts/build_stage3_input.py          # from stage2_v3.csv
    python scripts/build_stage3_input.py --test   # from stage2_v3_test.csv
"""

import re
import sys
from pathlib import Path

import pandas as pd
from filter_stage2_v3 import (
    ANIMAL_RE,
    BOILERPLATE_RE,
    FACT_SECTIONS,
    STRUCTURED_DIR,
    TAGS,
)
from judgment_parser import section
from tqdm import tqdm

PARQUET_DIR = "data/processed"
STAGE1_CSV = "data/filtered/stage1_v2.csv"
WINDOW = 300
MAX_CHARS = 3000
WS_RE = re.compile(r"\s+")


def passages(text: str) -> tuple[str, bool]:
    """Animal mentions with WINDOW chars of context; overlaps merged; capped."""
    text = BOILERPLATE_RE.sub("", WS_RE.sub("", text))
    spans: list[list[int]] = []
    for m in ANIMAL_RE.finditer(text):
        a, b = max(0, m.start() - WINDOW), min(len(text), m.end() + WINDOW)
        if spans and a <= spans[-1][1]:
            spans[-1][1] = max(spans[-1][1], b)
        else:
            spans.append([a, b])
    if not spans:  # e.g. 27-1 video cases phrased without listed animal words
        return text[:MAX_CHARS], len(text) > MAX_CHARS
    joined = "\n……\n".join(text[a:b] for a, b in spans)
    return joined[:MAX_CHARS], len(joined) > MAX_CHARS


def pick_text(full_text: str, row: dict) -> tuple[str, str]:
    facts = "".join(section(full_text, row, s) for s in FACT_SECTIONS)
    if facts.strip():
        return facts, "facts"
    reasons = section(full_text, row, "reasons")
    if reasons.strip():
        return reasons, "reasons"
    return full_text, "full_text"


def main():
    test = "--test" in sys.argv
    suffix = "_test" if test else ""
    s2 = pd.read_csv(f"data/filtered/stage2_v3{suffix}.csv")
    s1 = pd.read_csv(STAGE1_CSV)

    s1 = s1.rename(
        columns={"role": "stage1_role", "animal_conviction": "stage1_conviction"}
    )
    s1_cols = [
        "jid",
        "title",
        "batch",
        "stage1_role",
        "animal_basis",
        "stage1_conviction",
    ]
    s2_cols = ["jid", "title", "batch", *TAGS]
    cand = s1[s1_cols].merge(
        s2[s2_cols], on="jid", how="outer", suffixes=("", "_s2"), indicator=True
    )
    cand["in_stage1"] = cand["_merge"] != "right_only"
    cand["in_stage2"] = cand["_merge"] != "left_only"
    for col in ("title", "batch"):
        cand[col] = cand[col].fillna(cand[f"{col}_s2"])
    cand = cand.drop(columns=["_merge", "title_s2", "batch_s2"])
    cand[TAGS] = cand[TAGS].fillna(False).astype(bool)
    cand["stage1_conviction"] = cand["stage1_conviction"].fillna(False).astype(bool)
    cand["core"] = ~cand[TAGS].any(axis=1)

    cols = [
        "jid",
        *[f"{s}_{p}" for s in [*FACT_SECTIONS, "reasons"] for p in ("start", "end")],
    ]
    out = []
    for batch, group in tqdm(cand.groupby("batch"), desc="Extracting passages"):
        st = pd.read_parquet(f"{STRUCTURED_DIR}/{batch}", columns=cols).set_index("jid")
        texts = (
            pd.read_parquet(f"{PARQUET_DIR}/{batch}", columns=["jid", "full_text"])
            .drop_duplicates("jid")
            .set_index("jid")["full_text"]
        )
        for r in group.to_dict("records"):
            text = texts.get(r["jid"])
            if not isinstance(text, str) or r["jid"] not in st.index:
                out.append({**r, "passage_source": "missing", "passages": ""})
                continue
            src_text, src = pick_text(text, st.loc[r["jid"]].to_dict())
            p, truncated = passages(src_text)
            out.append(
                {
                    **r,
                    "passage_source": src,
                    "passages": p,
                    "passages_truncated": truncated,
                }
            )

    df = pd.DataFrame(out)
    print(f"Candidates: {len(df):,}")
    print(pd.crosstab(df["in_stage1"], df["in_stage2"], margins=True).to_string())
    print("\nPassage source:\n" + df["passage_source"].value_counts().to_string())
    print(
        f"Truncated at {MAX_CHARS} chars: {df['passages_truncated'].fillna(False).sum():,}"
    )
    print(f"Mean passage length: {df['passages'].str.len().mean():,.0f} chars")

    output = Path(f"data/filtered/stage3_input{suffix}.parquet")
    df.to_parquet(output, index=False)
    print(f"\nSaved: {output}")


if __name__ == "__main__":
    main()
