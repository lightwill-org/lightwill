"""
Stage 2 v3: keyword search over the facts sections of all criminal judgments.

Differences from v2 (filter_stage2_crime_first_fulltext.py):
- Searches only fact narratives (facts, combined 事實及理由, attach_facts of the attached
  indictment), located by data/structured. Prior records, sentencing reasons and statutes
  are not searched, which removes most of v2's noise.
- No crime-title filter: animal harm hidden behind drug, theft or fraud titles is kept.
- A sentence matches only if an animal word and a harm word are within PROXIMITY chars of
  each other inside the same sentence.
- Stage 1 v2 documents are scanned too (not skipped): convictions under the Animal
  Protection Act are known positives, so the share of them that match estimates recall.
- Stage 2 finds candidates; it does not judge them. Only text that cannot be facts is
  removed (fixed boilerplate, legal-norm sentences starting with 按). Likely noise is
  tagged, never dropped, and every match goes to the Stage 3 LLM:
    tag_quoted_or_product  all hits disappear once short quoted strings (nicknames,
                           handles, insults: 「阿猴」「狗哥」) and product words (熱狗,
                           狗食) are ignored
    tag_owner_or_traffic   every hit sentence is about an owner's animal hurting someone
                           (咬, 管束, 拴綁…) or swerving to avoid an animal (閃避)
    pigeon_ransom          pigeon-ransom extortion (擄鴿勒贖)
  `core` = none of the tags. Tags are used for reporting and for stratified sampling in
  Check 1, which tests whether the LLM rejects these cases on its own.
- Documents whose facts mention an animal word but have no proximity match are listed
  separately: Check 1 samples them to estimate what Stage 2 misses.
- Summary judgments often say the facts are in the attached indictment, but about two
  thirds of those attachments are not published. Such documents have no facts text;
  they are counted (facts_by_reference_only) as a coverage limit.

Universe: criminal documents (JID court code 4th char M), judgments and summary
judgments (rulings excluded), titles without 野生動物, with at least one facts section.

Outputs (no text, no names):
    data/filtered/stage2_v3.csv                  matched documents
    data/filtered/stage2_v3_stage1_missed.csv    Stage 1 convictions that did not match
    data/filtered/stage2_v3_animal_no_match.csv  animal word in facts, no match
    (test mode writes *_test.csv instead)

Usage:
    python scripts/filter_stage2_v3.py --test 300   # 300 spread batches + all Stage 1 batches
    python scripts/filter_stage2_v3.py              # all batches
"""

import glob
import os
import re
import sys
from collections import Counter
from multiprocessing import Pool
from pathlib import Path

import pandas as pd
from judgment_parser import section
from resource_monitor import start_memory_guard
from tqdm import tqdm

PARQUET_DIR = "data/processed"
STRUCTURED_DIR = "data/structured"
STAGE1_CSV = "data/filtered/stage1_v2.csv"
OUTPUT_DIR = Path("data/filtered")

FACT_SECTIONS = ["facts", "combined", "attach_facts"]
PROXIMITY = 30

# 龜 and bare 鳥 are left out: they mostly match place names (龜山, 鳥松)
ANIMAL_WORDS = [
    "動物",
    "寵物",
    "家畜",
    "家禽",
    "牲畜",
    "犬",
    "狗",
    "貓",
    "豬隻",
    "牛隻",
    "羊隻",
    "山羊",
    "雞隻",
    "鴨隻",
    "鵝隻",
    "馬匹",
    "兔",
    "鳥類",
    "小鳥",
    "鳥隻",
    "鸚鵡",
    "猴",
    "鴿",
    "鬥雞",
    # Dog breeds that appear without 犬/狗
    "吉娃娃",
    "貴賓",
    "博美",
    "馬爾濟斯",
    "臘腸",
    "哈士奇",
    "米克斯",
]
HARM_WORDS = [
    # Direct violence
    "打",
    "踢",
    "踹",
    "踩",
    "摔",
    "丟",
    "扔",
    "拋",
    "毆",
    "虐",
    "殺",
    "宰",
    "刺",
    "砍",
    "割",
    "剁",
    "燒",
    "燙",
    "勒",
    "吊",
    "綁",
    "悶",
    "淹",
    "溺",
    "埋",
    "撞",
    "砸",
    "擊",
    "鞭",
    "戳",
    "捅",
    "夾",
    "鬥",  # 鬥狗, 鬥雞 (fighting, also in videos under article 27-1)
    # Poison and weapons (藥 is left out: matches 動物用藥品)
    "毒",
    "射",
    "槍",
    # Outcome
    "傷",
    "死",
    "斃",
    "亡",
    # Neglect and abandonment
    "棄",
    "餓",
    # Threats
    "揚言",
    "威脅",
    "恐嚇",
]

_ANIMAL = "|".join(ANIMAL_WORDS)
_HARM = "|".join(HARM_WORDS)
PROXIMITY_RE = re.compile(
    rf"(?:{_ANIMAL})[^。！？]{{0,{PROXIMITY}}}?(?:{_HARM})"
    rf"|(?:{_HARM})[^。！？]{{0,{PROXIMITY}}}?(?:{_ANIMAL})"
)
ANIMAL_RE = re.compile(_ANIMAL)
HARM_RE = re.compile(_HARM)
SENTENCE_RE = re.compile(r"[^。！？]+")
WS_RE = re.compile(r"\s+")

# Removed before matching: fixed phrasing that cannot describe a real animal
BOILERPLATE_RE = re.compile(
    r"穿入豬隻皮肉層"  # ballistics standard: live-pig shooting test
    r"|野生動物保育法案件必須鑑定是否屬於保育類動物"
    r"|其他動物力"  # drunk-driving statute wording: 以人力或其他動物力之交通工具
)
# Tagging only (see module docstring)
QUOTED_OR_PRODUCT_RE = re.compile(
    r"「[^」]{1,6}」|熱狗|黑貓宅急便|狗食|貓食|狗糧|貓糧|貓砂|狗罐|貓罐|狗仔|海狗"
)
OWNER_OR_TRAFFIC_RE = re.compile(r"咬|管束|拴|栓|繫牢|牽繩|狗鍊|狗繩|閃避|閃躲")
PIGEON_RANSOM_RE = re.compile(r"擄鴿|鴿[^。]{0,60}贖|贖[^。]{0,60}鴿")
BY_REFERENCE_RE = re.compile(r"引用.{0,40}附件|附件.{0,20}之?記載")

STRUCT_COLS = [
    "jid",
    "status",
    "doc_kind",
    "main_guilty",
    *[f"{s}_{p}" for s in FACT_SECTIONS for p in ("start", "end")],
]
TAGS = ["tag_quoted_or_product", "tag_owner_or_traffic", "pigeon_ransom"]
COURT_LEVELS = {"D": "district", "E": "summary_court", "H": "high", "S": "supreme"}


def scan_text(text: str) -> dict | None:
    """
    Return match details for one facts text, None if nothing matched, or {} if an
    animal word appears but nothing matched (for the miss-estimate list).
    """
    text = BOILERPLATE_RE.sub("", WS_RE.sub("", text))
    hits = unquoted = not_owner = 0
    animals, harms = Counter(), Counter()
    for sent in SENTENCE_RE.findall(text):
        if sent.startswith("按"):  # statement of law, not facts
            continue
        found = PROXIMITY_RE.findall(sent)
        if not found:
            continue
        hits += 1
        unquoted += bool(PROXIMITY_RE.search(QUOTED_OR_PRODUCT_RE.sub("", sent)))
        not_owner += not OWNER_OR_TRAFFIC_RE.search(sent)
        for m in found:
            animals.update(set(ANIMAL_RE.findall(m)))
            harms.update(set(HARM_RE.findall(m)))
    if not hits:
        return {} if ANIMAL_RE.search(text) else None
    return {
        "n_hit_sentences": hits,
        "tag_quoted_or_product": unquoted == 0,
        "tag_owner_or_traffic": not_owner == 0,
        "pigeon_ransom": bool(PIGEON_RANSOM_RE.search(text)),
        "animal_terms": ",".join(w for w, _ in animals.most_common()),
        "harm_terms": ",".join(w for w, _ in harms.most_common()),
    }


def process_batch(name: str) -> tuple[list[dict], list[dict], dict]:
    """Scan one batch; return matched rows, animal-but-no-match rows, counts."""
    st = pd.read_parquet(f"{STRUCTURED_DIR}/{name}", columns=STRUCT_COLS)
    code = st["jid"].str.split(",").str[0]
    st = st[
        (code.str[3] == "M") & st["doc_kind"].isin(["judgment", "summary_judgment"])
    ]
    has_facts = (st[[f"{s}_start" for s in FACT_SECTIONS]] >= 0).any(axis=1)
    counts = {"judgments": len(st), "with_facts": int(has_facts.sum())}
    st = st[has_facts]
    if st.empty:
        return [], [], counts

    texts = pd.read_parquet(
        f"{PARQUET_DIR}/{name}", columns=["jid", "title", "full_text"]
    ).drop_duplicates("jid")
    st = st.merge(texts, on="jid", how="inner")
    st = st[~st["title"].fillna("").str.contains("野生動物")]

    rows, no_match = [], []
    counts["facts_by_reference_only"] = 0
    for r in st.itertuples(index=False):
        if not isinstance(r.full_text, str):
            continue
        rd = r._asdict()
        facts = "".join(section(r.full_text, rd, s) for s in FACT_SECTIONS)
        if r.attach_facts_start < 0 and BY_REFERENCE_RE.search(
            WS_RE.sub("", facts[:300])
        ):
            counts["facts_by_reference_only"] += 1
        found = scan_text(facts)
        if found == {}:
            no_match.append({"jid": r.jid, "title": r.title, "batch": name})
        elif found:
            rows.append(
                {
                    "jid": r.jid,
                    "title": r.title,
                    "doc_kind": r.doc_kind,
                    "main_guilty": r.main_guilty,
                    "facts_chars": len(facts),
                    **found,
                    "batch": name,
                }
            )
    return rows, no_match, counts


def load_stage1() -> pd.DataFrame:
    s1 = pd.read_csv(STAGE1_CSV, dtype={"year": str})
    if "animal_conviction" not in s1 or "batch" not in s1:
        sys.exit(
            "stage1_v2.csv lacks animal_conviction/batch. "
            "Re-run: python scripts/build_stage1_v2.py"
        )
    return s1


def report(df: pd.DataFrame, s1: pd.DataFrame, scanned: list[str], counts: Counter):
    s1_scanned = s1[s1["batch"].isin(scanned)]
    conv = s1_scanned[s1_scanned["animal_conviction"] & s1_scanned["has_facts"]]
    hit = conv["jid"].isin(df["jid"])
    hit_core = conv["jid"].isin(df.loc[df["core"], "jid"])

    print(f"\nJudgments scanned: {counts['judgments']:,}")
    print(f"With a facts section: {counts['with_facts']:,}")
    print(
        "  of which facts only by reference to an unpublished attachment: "
        f"{counts['facts_by_reference_only']:,}"
    )
    print(f"Animal word in facts but no match: {counts['animal_no_match']:,}")
    print(f"Matched (all go to Stage 3): {len(df):,}")

    print("\nRecall on Stage 1 convictions under the act (with facts):")
    print(f"  matched:          {hit.sum()}/{len(conv)} = {hit.mean() * 100:.1f}%")
    print(
        f"  matched, no tag:  {hit_core.sum()}/{len(conv)} = {hit_core.mean() * 100:.1f}%"
    )

    new = df[~df["jid"].isin(s1["jid"])]
    print(f"\nMatched outside Stage 1: {len(new):,}")
    print("\nTags (outside Stage 1; a document can have several):")
    for tag in TAGS:
        print(f"  {tag:<24}{new[tag].sum():>7,}")
    print(f"  {'core (no tag)':<24}{new['core'].sum():>7,}")
    print("\nBy court level:")
    print(new["court_level"].value_counts().to_string())
    print("\nTop titles (outside Stage 1), with share of core:")
    titles = new.groupby("title")["core"].agg(n="size", core_share="mean")
    print(titles.sort_values("n", ascending=False).head(30).round(2).to_string())

    known = df[df["jid"].isin(conv["jid"])]

    def term_share(frame: pd.DataFrame, col: str) -> pd.Series:
        return frame[col].str.split(",").explode().value_counts() / max(len(frame), 1)

    for col in ["animal_terms", "harm_terms"]:
        table = pd.DataFrame(
            {
                "stage1_conv": term_share(known, col),
                "outside_stage1": term_share(new, col),
                "n_outside": new[col].str.split(",").explode().value_counts(),
            }
        ).fillna(0)
        print(f"\n{col}: share of documents containing the term")
        print(
            table.sort_values("n_outside", ascending=False)
            .round(3)
            .head(40)
            .to_string()
        )
    return conv[~hit]


def main():
    start_memory_guard(10)
    s1 = load_stage1()
    files = sorted(Path(f).name for f in glob.glob(f"{STRUCTURED_DIR}/*.parquet"))

    test = sys.argv[1:2] == ["--test"]
    suffix = ""
    if test:
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 300
        spread = files[:: max(1, len(files) // n)][:n]
        files = sorted(set(spread) | set(s1["batch"]))
        suffix = "_test"
        print(
            f"⚠️  Test mode: {len(spread)} spread batches + Stage 1 batches "
            f"= {len(files)} files"
        )

    workers = max(1, (os.cpu_count() or 2) - 1)
    rows, no_match, counts = [], [], Counter()
    with Pool(workers) as pool:
        for r, nm, c in tqdm(
            pool.imap_unordered(process_batch, files), total=len(files), desc="Scanning"
        ):
            rows.extend(r)
            no_match.extend(nm)
            counts.update(c)
    counts["animal_no_match"] = len(no_match)

    df = pd.DataFrame(rows)
    parts = df["jid"].str.split(",")
    df["court_level"] = parts.str[0].str[2].map(COURT_LEVELS).fillna("other")
    df["year"] = parts.str[4].str[:4]
    df["in_stage1"] = df["jid"].isin(s1["jid"])
    df["stage1_conviction"] = df["jid"].isin(s1.loc[s1["animal_conviction"], "jid"])
    df["core"] = ~df[TAGS].any(axis=1)

    missed = report(df, s1, files, counts)
    if test:
        spread_hits = df[df["batch"].isin(spread) & ~df["in_stage1"]]
        scale = len(glob.glob(f"{STRUCTURED_DIR}/*.parquet")) / len(spread)
        print(
            f"\nEstimated over all batches, outside Stage 1: "
            f"matched ~{len(spread_hits) * scale:,.0f} "
            f"(core ~{spread_hits['core'].sum() * scale:,.0f})"
        )

    out = OUTPUT_DIR / f"stage2_v3{suffix}.csv"
    df.sort_values("jid").to_csv(out, index=False)
    missed_out = OUTPUT_DIR / f"stage2_v3_stage1_missed{suffix}.csv"
    missed[["jid", "title", "role", "animal_basis", "batch"]].to_csv(
        missed_out, index=False
    )
    no_match_out = OUTPUT_DIR / f"stage2_v3_animal_no_match{suffix}.csv"
    pd.DataFrame(no_match).to_csv(no_match_out, index=False)
    print(f"\nSaved: {out}\nSaved: {missed_out}\nSaved: {no_match_out}")


if __name__ == "__main__":
    main()
