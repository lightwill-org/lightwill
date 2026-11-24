"""
Matched control comparison: are Animal Protection Act defendants different from other
criminal defendants in their prior records (backward) and later offences (forward)?

Design
- Universe: first-instance trial documents (district / summary courts; case words for
  appeals, rulings and petitions excluded)
- Exposed: Stage 1 judgments in the universe (Wildlife Conservation Act titles excluded:
  poaching is not the animal abuse this study targets)
- Controls: for each exposed judgment, k random documents (default 10) from the same
  court code and judgment year, whose title does not mention animals and is not procedural
- Both groups use exactly the same resolved links, so linking errors affect both equally

Link filters
- match == normalized_word dropped (only 35% name agreement in validation)
- links where both sides have names but none is shared are dropped (likely wrong person)
- the other side of a link must not be a procedural ruling (定應執行刑, 撤銷緩刑, ...)

Outcomes are counted per judgment (any link of that kind), not per link.
Risk ratios use the log method for 95% CIs; small counts give wide intervals.

Outputs (no names):
- data/analysis/control_comparison.csv  outcome table
- data/analysis/control_sample.csv      jid, group, outcome flags

Usage:
    python scripts/build_control_comparison.py [k_controls] [memory_limit_gb]
"""

import math
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from analyze_stage1_linkage import STAGE1_CSV, SUMMARY_CSV, name_shared
from resource_monitor import start_memory_guard

INDEX_DIR = "data/index"
OUT_DIR = Path("data/analysis")
SEED = 42

FIRST_INSTANCE_COURT_LEVELS = {
    "D",
    "E",
}  # 3rd char of court code: D = district, E = summary
NON_TRIAL_WORD_RE = re.compile(r"上|抗|聲|撤|再|非|附民|助|保|執")
PROCEDURAL_RE = re.compile(
    r"定應執行|定執行刑|撤銷緩刑|緩刑|保護管束|聲明異議|易科|易服|再審|假釋|沒收|"
    r"強制工作|觀察勒戒|強制戒治|停止執行|聲請|更正|補充判決|附帶民"
)
ANIMAL_RE = re.compile(r"動物")
WILDLIFE_RE = re.compile(r"野生動物")

# Outcome categories by title keyword (a title can fall into several)
CATEGORIES = {
    "public_danger": r"公共危險",
    "drugs": r"毒品|麻醉藥品|煙毒",
    "theft": r"竊盜",
    "fraud": r"詐欺",
    "violence": r"殺人|傷害|家庭暴力|家暴|強盜|搶奪|恐嚇|妨害自由|強制|擄人|性自主|性騷擾",
    "animal": r"動物保護",
}
CATEGORY_RES = {k: re.compile(v) for k, v in CATEGORIES.items()}


def wilson(x: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return float("nan"), float("nan")
    p = x / n
    denom = 1 + z**2 / n
    centre = (p + z**2 / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / denom
    return centre - half, centre + half


def risk_ratio(a: int, n1: int, c: int, n2: int) -> tuple[float, float, float]:
    if a == 0 and c == 0:  # no events in either group: ratio undefined
        return float("nan"), float("nan"), float("nan")
    if a == 0 or c == 0:  # Haldane correction
        a, c, n1, n2 = a + 0.5, c + 0.5, n1 + 1, n2 + 1
    rr = (a / n1) / (c / n2)
    se = math.sqrt(1 / a - 1 / n1 + 1 / c - 1 / n2)
    return rr, rr * math.exp(-1.96 * se), rr * math.exp(1.96 * se)


def load_links() -> pd.DataFrame:
    links = pd.read_parquet(
        f"{INDEX_DIR}/prior_case_refs_resolved.parquet",
        columns=["jid", "cited_jid", "match", "resolved"],
    )
    links = links[links["resolved"] & (links["match"] != "normalized_word")]
    links = links[["jid", "cited_jid"]].drop_duplicates().reset_index(drop=True)

    defendants = pd.read_parquet(
        f"{INDEX_DIR}/defendants.parquet", columns=["jid", "name_hash"]
    )
    shared = name_shared(links, defendants.drop_duplicates()).values
    n_before = len(links)
    links = links[pd.Series(shared).fillna(True).astype(bool).values].reset_index(
        drop=True
    )
    print(f"Links: {n_before:,} → {len(links):,} after dropping name mismatches")
    return links


def scan_summary(link_jids: set[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (universe of first-instance trials, title info for jids in links)."""
    universe, linked = [], []
    for chunk in pd.read_csv(
        SUMMARY_CSV, usecols=["jid", "title"], chunksize=500_000, dtype=str
    ):
        chunk = chunk.dropna(subset=["jid"]).fillna({"title": ""})
        chunk = chunk.drop_duplicates("jid")
        parts = chunk["jid"].str.split(",")
        code, word, date = parts.str[0], parts.str[2], parts.str[4]

        in_links = chunk[chunk["jid"].isin(link_jids)]
        linked.append(in_links)

        trial = code.str[2].isin(FIRST_INSTANCE_COURT_LEVELS) & ~word.fillna(
            ""
        ).str.contains(NON_TRIAL_WORD_RE)
        u = chunk[trial].assign(court=code[trial], year=date[trial].str[:4])
        universe.append(u[["jid", "court", "year", "title"]])

    universe = pd.concat(universe, ignore_index=True).drop_duplicates("jid")
    linked = (
        pd.concat(linked, ignore_index=True)
        .drop_duplicates("jid")
        .set_index("jid")["title"]
    )
    return universe, linked


def title_flags(titles: pd.Series) -> pd.DataFrame:
    flags = pd.DataFrame(index=titles.index)
    flags["procedural"] = titles.str.contains(PROCEDURAL_RE)
    for name, pattern in CATEGORY_RES.items():
        flags[name] = titles.str.contains(pattern)
    return flags


def sample_controls(
    universe: pd.DataFrame, exposed: pd.DataFrame, k: int
) -> pd.DataFrame:
    pool = universe[
        ~universe["title"].str.contains(ANIMAL_RE)
        & ~universe["title"].str.contains(PROCEDURAL_RE)
    ]
    need = exposed.groupby(["court", "year"]).size() * k
    rng = np.random.default_rng(SEED)
    picked, short = [], 0
    for (court, year), group in pool.groupby(["court", "year"]):
        n = need.get((court, year), 0)
        if n == 0:
            continue
        take = min(n, len(group))
        short += n - take
        picked.append(group.iloc[rng.choice(len(group), size=take, replace=False)])
    missing_strata = set(need.index) - set(pool.groupby(["court", "year"]).groups)
    short += sum(need[s] for s in missing_strata)
    controls = pd.concat(picked, ignore_index=True)
    print(
        f"Controls: {len(controls):,} sampled (target {need.sum():,}, short {short:,})"
    )
    return controls


def outcomes(
    study: pd.DataFrame, links: pd.DataFrame, flags: pd.DataFrame
) -> pd.DataFrame:
    jids = set(study["jid"])
    result = study[["jid", "group"]].set_index("jid")

    for direction, anchor, other in [
        ("prior", "jid", "cited_jid"),
        ("later", "cited_jid", "jid"),
    ]:
        sub = links[links[anchor].isin(jids)]
        # Unknown title (not in summary): treated as non-procedural with no category
        f = flags.reindex(sub[other]).fillna(False).astype(bool).reset_index(drop=True)
        keep = ~f["procedural"].values
        sub, f = sub[keep].reset_index(drop=True), f[keep].reset_index(drop=True)
        f[anchor] = sub[anchor].values
        f["any"] = True
        agg = f.groupby(anchor)[["any", *CATEGORIES]].any()
        for col in agg.columns:
            result[f"{direction}_{col}"] = (
                agg[col].reindex(result.index, fill_value=False).astype(bool)
            )
    return result.reset_index()


def compare(result: pd.DataFrame) -> pd.DataFrame:
    exp = result[result["group"] == "animal_protection"]
    ctl = result[result["group"] == "control"]
    rows = []
    for col in [c for c in result.columns if c.startswith(("prior_", "later_"))]:
        a, n1, c, n2 = int(exp[col].sum()), len(exp), int(ctl[col].sum()), len(ctl)
        lo1, hi1 = wilson(a, n1)
        rr, rr_lo, rr_hi = risk_ratio(a, n1, c, n2)
        rows.append(
            {
                "outcome": col,
                "exposed_n": a,
                "exposed_pct": a / n1 * 100,
                "exposed_ci": f"{lo1 * 100:.1f}-{hi1 * 100:.1f}",
                "control_n": c,
                "control_pct": c / n2 * 100,
                "risk_ratio": rr,
                "rr_ci": f"{rr_lo:.2f}-{rr_hi:.2f}",
                "significant": rr_lo > 1 or rr_hi < 1,
            }
        )
    return pd.DataFrame(rows)


def main():
    k = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    limit_gb = float(sys.argv[2]) if len(sys.argv) > 2 else 8.0
    start_memory_guard(limit_gb)
    print(f"Controls per case: {k} | memory limit: {limit_gb:g} GB")

    links = load_links()
    universe, linked_titles = scan_summary(set(links["jid"]) | set(links["cited_jid"]))
    print(f"First-instance trial documents: {len(universe):,}")
    flags = title_flags(linked_titles)

    stage1 = pd.read_csv(
        STAGE1_CSV, usecols=["jid", "animal_case_stage1"], low_memory=False
    )
    stage1_jids = set(stage1.loc[stage1["animal_case_stage1"] == True, "jid"])  # noqa: E712
    exposed = universe[universe["jid"].isin(stage1_jids)]
    n_first = len(exposed)
    exposed = exposed[~exposed["title"].str.contains(WILDLIFE_RE)]
    print(
        f"Stage 1: {len(stage1_jids):,} → first-instance trials {n_first:,} "
        f"→ excluding wildlife act {len(exposed):,}"
    )

    controls = sample_controls(universe, exposed, k)
    study = pd.concat(
        [exposed.assign(group="animal_protection"), controls.assign(group="control")],
        ignore_index=True,
    )
    del universe

    result = outcomes(study, links, flags)
    table = compare(result)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUT_DIR / "control_sample.csv", index=False)
    table.to_csv(OUT_DIR / "control_comparison.csv", index=False)

    print(f"\nAnimal protection n={len(exposed):,} | control n={len(controls):,}")
    with pd.option_context(
        "display.width", 200, "display.float_format", "{:.2f}".format
    ):
        print(table.to_string(index=False))
    print(
        "\nprior_* = cited priors (backward); later_* = later documents citing it (forward)"
    )
    print("significant = 95% CI of risk ratio excludes 1")
    print(
        f"Saved: {OUT_DIR / 'control_comparison.csv'}, {OUT_DIR / 'control_sample.csv'}"
    )


if __name__ == "__main__":
    main()
