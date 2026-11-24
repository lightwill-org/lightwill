"""
Stage 2: Crime-First Fulltext Search (v2)

Finds crime judgments whose full text mentions animals together with harm-related
words. Recall-oriented: false positives are removed later by the Stage 3 LLM.

Changes in v2:
- Crime list adds robbery, snatching, firearms
- Drugs and theft are NOT included: a 500-batch sample showed their animal mentions are
  nicknames (阿狗), pet-food theft, etc. Their role in the study (escalation outcome) is
  covered by the prior-record regex search over all judgments instead.
- Firearms-only cases require an animal word within 30 chars of a shooting/harm word,
  because their harm words (槍, 射) always match
- Standard ballistics boilerplate (穿入豬隻皮肉層) is stripped before matching
- Animal words use compounds (豬隻, 牛隻...) instead of single characters to avoid surname/food noise
- Harm words add neglect/abandonment and poisoning/shooting
- No exclusion keywords: a judgment can mention 狗咬 and still contain abuse by the defendant
- Records which crime and animal keywords matched, for prioritization

Usage:
    python scripts/filter_stage2_crime_first_fulltext.py [batch_limit]
"""

import glob
import re
import sys
from pathlib import Path

import pandas as pd

PARQUET_DIR = "data/processed"
STAGE1_CSV = "data/filtered/criminal_judgments_tagged_20261002_103027.csv"
STAGE2_FILE = "data/filtered/stage2_crime_first_fulltext_v2.csv"
FINAL_CSV = "data/filtered/criminal_judgments_tagged_20261002_103027_final_v2.csv"

FIREARMS_KW = "槍砲"

CRIME_KEYWORDS = [
    # Homicide and assault
    "殺人",
    "傷害",
    "重傷害",
    # Domestic violence
    "家庭暴力",
    "家暴",
    # Threats and coercion
    "恐嚇",
    "強制",
    "脅迫",
    "妨害自由",
    # Public danger
    "公共危險",
    "放火",
    # Property damage (harming another person's animal is often charged as 毀損)
    "毀損",
    "毀棄",
    "損壞",
    # Robbery
    "強盜",
    "搶奪",
    # Weapons (some animal shootings are charged under the firearms act)
    FIREARMS_KW,
    # Other
    "虐待",
    "凌虐",
]

ANIMAL_KEYWORDS = [
    # General
    "動物",
    "寵物",
    "家畜",
    "家禽",
    "牲畜",
    # Dogs
    "犬",
    "狗",
    # Cats
    "貓",
    # Livestock and others (compounds only; single characters are too noisy)
    "豬隻",
    "牛隻",
    "羊隻",
    "雞隻",
    "鴨隻",
    "鵝隻",
    "馬匹",
    "兔子",
    "鸚鵡",
    "鳥類",
    "猴子",
]

HARM_KEYWORDS = [
    # Direct violence
    "打",
    "踢",
    "摔",
    "丟",
    "扔",
    "毆",
    "虐",
    "殺",
    "宰",
    "刺",
    "砍",
    "切",
    "燒",
    "燙",
    "勒",
    "吊",
    "活埋",
    "傷害",
    # Poison and weapons
    "毒",
    "射",
    "槍",
    # Neglect and abandonment
    "遺棄",
    "棄養",
    "棄置",
    "餓",
    # Threats
    "威脅",
    "揚言",
]

CRIME_RE = "|".join(CRIME_KEYWORDS)
ANIMAL_RE = "|".join(ANIMAL_KEYWORDS)
HARM_RE = "|".join(HARM_KEYWORDS)

# Standard legal phrasing that mentions animals without any animal being involved
BOILERPLATE_RE = re.compile(
    r"穿入豬隻皮肉層"  # ballistics standard: live-pig shooting test
    r"|野生動物保育法案件必須鑑定是否屬於保育類動物"  # Ministry of Justice expert-witness guideline
)

PROXIMITY_HARM = "射|槍|殺|打|傷|死|擊"
PROXIMITY_RE = re.compile(
    rf"(?:{ANIMAL_RE}).{{0,30}}(?:{PROXIMITY_HARM})|(?:{PROXIMITY_HARM}).{{0,30}}(?:{ANIMAL_RE})",
    re.S,
)
OTHER_CRIME_RE = "|".join(kw for kw in CRIME_KEYWORDS if kw != FIREARMS_KW)


def matched_keywords(text: str, keywords: list[str]) -> str:
    return ",".join(kw for kw in keywords if kw in text)


def search_stage2(batch_limit: int | None = None) -> pd.DataFrame:
    print("=" * 60)
    print("Stage 2 (v2): Crime-First Fulltext Search")
    print("=" * 60)
    print(
        f"Crime keywords ({len(CRIME_KEYWORDS)}), animal keywords ({len(ANIMAL_KEYWORDS)}), "
        f"harm keywords ({len(HARM_KEYWORDS)})"
    )

    parquet_files = sorted(glob.glob(f"{PARQUET_DIR}/*_batch*.parquet"))
    if batch_limit:
        parquet_files = parquet_files[:batch_limit]
        print(f"⚠️  Test mode: first {batch_limit} batches")
    print(f"Parquet files: {len(parquet_files):,}")

    stage1 = pd.read_csv(
        STAGE1_CSV, usecols=["jid", "animal_case_stage1"], low_memory=False
    )
    stage1_jids = set(stage1.loc[stage1["animal_case_stage1"] == True, "jid"])  # noqa: E712
    print(f"Stage 1 JIDs to skip: {len(stage1_jids):,}")

    matched = []
    total = crime_total = 0

    for i, pf in enumerate(parquet_files, 1):
        df = pd.read_parquet(
            pf,
            columns=[
                "jid",
                "court_code",
                "year",
                "case_category",
                "title",
                "judgment_date",
                "full_text",
            ],
        )
        total += len(df)

        df = df[df["title"].str.contains(CRIME_RE, na=False)]
        crime_total += len(df)
        df = df[~df["jid"].isin(stage1_jids)]
        df = df[df["full_text"].str.contains(ANIMAL_RE, na=False)]

        if len(df):
            df = df.copy()
            df["full_text"] = df["full_text"].str.replace(
                BOILERPLATE_RE, "", regex=True
            )
            df = df[df["full_text"].str.contains(ANIMAL_RE, na=False)]
            df = df[df["full_text"].str.contains(HARM_RE, na=False)]

            firearms_only = ~df["title"].str.contains(OTHER_CRIME_RE, na=False)
            near = df["full_text"].str.contains(PROXIMITY_RE, na=False)
            df = df[~firearms_only | near]

        if len(df):
            df = df.copy()
            df["matched_crime_kw"] = df["title"].map(
                lambda t: matched_keywords(t, CRIME_KEYWORDS)
            )
            df["matched_animal_kw"] = df["full_text"].map(
                lambda t: matched_keywords(t, ANIMAL_KEYWORDS)
            )
            matched.append(df.drop(columns=["full_text"]))

        if i % 500 == 0 or i == len(parquet_files):
            n = sum(len(m) for m in matched)
            print(
                f"  [{i:,}/{len(parquet_files):,}] scanned {total:,} | crime {crime_total:,} | matched {n:,}"
            )

    result = pd.concat(matched, ignore_index=True).drop_duplicates(subset="jid")
    print(f"\nTotal scanned: {total:,}")
    print(f"Crime-title cases: {crime_total:,}")
    print(f"Stage 2 candidates: {len(result):,}")
    return result


def print_breakdown(df: pd.DataFrame):
    print("\nCandidates by crime keyword (a case can match several):")
    crime_counts = df["matched_crime_kw"].str.split(",").explode().value_counts()
    print(crime_counts.to_string())

    print("\nCandidates by animal keyword (a case can match several):")
    animal_counts = df["matched_animal_kw"].str.split(",").explode().value_counts()
    print(animal_counts.to_string())


def merge_to_tagged(df_stage2: pd.DataFrame):
    stage2_jids = set(df_stage2["jid"])
    kw_map = df_stage2.set_index("jid")["matched_crime_kw"]
    first = True
    updated = 0

    for chunk in pd.read_csv(
        STAGE1_CSV,
        chunksize=100_000,
        low_memory=False,
        dtype={"matched_keywords_stage1": str},
    ):
        mask = chunk["jid"].isin(stage2_jids)
        chunk["animal_case_stage2"] = mask
        chunk["matched_keywords_stage2"] = chunk["jid"].map(kw_map).where(mask, "")
        updated += mask.sum()
        chunk.to_csv(FINAL_CSV, index=False, mode="w" if first else "a", header=first)
        first = False

    print(f"\n✓ Tagged {updated:,} rows → {FINAL_CSV}")


if __name__ == "__main__":
    batch_limit = int(sys.argv[1]) if len(sys.argv) > 1 else None

    df_stage2 = search_stage2(batch_limit)
    Path(STAGE2_FILE).parent.mkdir(parents=True, exist_ok=True)
    df_stage2.to_csv(STAGE2_FILE, index=False)
    print(f"✓ Saved: {STAGE2_FILE}")

    print_breakdown(df_stage2)

    if batch_limit:
        print("\nTest mode: skipping merge into the full tagged CSV")
    else:
        merge_to_tagged(df_stage2)
