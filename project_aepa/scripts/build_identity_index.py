"""
Build cross-judgment linking indexes over all judgments.

Outputs (data/index/):
- defendants.parquet      one row per (judgment, defendant): jid, court_code, year,
                          judgment_date, doc_type, name_hash
- name_freq.parquet       per name_hash: number of documents, courts, first/last year
- prior_case_refs.parquet case numbers cited in prior-record text: jid, cited_court,
                          cited_year, cited_word, cited_no, ref_type
- court_names.parquet     court name in document headers → court code (from jid),
                          used to resolve cited case numbers to JIDs

Defendant names are never stored in plain text. They are hashed with HMAC-SHA256 using
NAME_HASH_KEY from .env, so hashes cannot be reversed by hashing a list of common names.

Only the header (text before 主文) is read for names. Masked names (甲○○) are skipped.

Usage:
    python scripts/build_identity_index.py [n_files]
"""

import glob
import hashlib
import hmac
import os
import re
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

PARQUET_DIR = "data/processed"
OUTPUT_DIR = Path("data/index")

HEADER_END_RE = re.compile(r"主[\s　]*文")
DEFENDANT_RE = re.compile(r"被[\s　]*告[\s　]+([^\s　，,（(]{1,12})")
MASKED_RE = re.compile(r"[○◯〇Ｏ０0]")
FULL_NAME_RE = re.compile(r"^[一-鿿]{2,4}$")

NUM = r"[\d０-９一二三四五六七八九十百千零○〇]"
PRIOR_RE = re.compile(r"(?:前|曾)(?:於[^，。]{0,20})?因[^。]{0,40}?案件")
CASE_NO_RE = re.compile(rf"({NUM}+)年度([^\d，。、；：（）()]{{1,8}}?)字第({NUM}+)")
COURT_RE = re.compile(
    r"(?:臺灣|台灣|福建)[一-鿿]{0,6}?(?:地方法院|高等法院(?:[一-鿿]{1,4}?分院)?)"
    r"|最高法院|本院|同院|同法院"
)
HEADER_COURT_RE = re.compile(
    r"^((?:臺灣|台灣|福建)[一-鿿]{0,6}?(?:地方法院|高等法院(?:[一-鿿]{1,4}?分院)?)|最高法院)"
)
# Prosecutor (偵) and execution (執) numbers belong to prosecutors' offices, not court judgments
PROSECUTOR_WORD_RE = re.compile(r"偵|執")
WS_RE = re.compile(r"[\s　]+")

CN_DIGITS = {c: i for i, c in enumerate("零一二三四五六七八九")} | {"○": 0, "〇": 0}
CN_UNITS = {"十": 10, "百": 100, "千": 1000}
FULLWIDTH = str.maketrans("０１２３４５６７８９", "0123456789")

DEFENDANT_SCHEMA = pa.schema(
    [
        ("jid", pa.string()),
        ("court_code", pa.string()),
        ("year", pa.int32()),
        ("judgment_date", pa.string()),
        ("doc_type", pa.string()),
        ("name_hash", pa.string()),
    ]
)
PRIOR_SCHEMA = pa.schema(
    [
        ("jid", pa.string()),
        ("cited_court", pa.string()),
        ("cited_year", pa.string()),
        ("cited_word", pa.string()),
        ("cited_no", pa.string()),
        ("ref_type", pa.string()),
    ]
)


def to_arabic(s: str) -> str:
    """Convert 104 / １０４ / 一○四 / 九十 / 一百零二 to '104', '90', '102'. Returns '' on failure."""
    s = s.translate(FULLWIDTH)
    if s.isdigit():
        return str(int(s))
    if any(c in CN_UNITS for c in s):
        total, current = 0, 0
        for c in s:
            if c in CN_DIGITS:
                current = CN_DIGITS[c]
            elif c in CN_UNITS:
                total += (current or 1) * CN_UNITS[c]
                current = 0
            else:
                return ""
        return str(total + current)
    if all(c in CN_DIGITS for c in s):
        return str(int("".join(str(CN_DIGITS[c]) for c in s)))
    return ""


def hash_name(name: str, key: bytes) -> str:
    return hmac.new(key, name.encode("utf-8"), hashlib.sha256).hexdigest()[:20]


def doc_type(text: str) -> str:
    head = text[:200]
    if "判決" in head:
        return "判決"
    if "裁定" in head:
        return "裁定"
    return "other"


def parse_jid(jid: str) -> tuple[str, int, str]:
    parts = jid.split(",")
    year = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else -1
    date = parts[4] if len(parts) > 4 else ""
    return parts[0], year, date


def extract_names(text: str) -> list[str]:
    end = HEADER_END_RE.search(text[:1500])
    header = text[: end.start()] if end else text[:800]
    names = []
    for raw in DEFENDANT_RE.findall(header):
        if MASKED_RE.search(raw) or not FULL_NAME_RE.match(raw):
            continue
        if raw not in names:
            names.append(raw)
    return names


def extract_prior_refs(text: str) -> list[tuple[str, str, str, str, str]]:
    """
    Court names are resolved by carrying the last court seen forward, because judgments
    often name the court once and then list several case numbers ("…同院…號").
    Returns cited_court as a full court name, "本院" (the citing court), or "".
    """
    refs = []
    for m in PRIOR_RE.finditer(text):
        # Hard line breaks can split case numbers (e.g. 第5\n87號), so remove whitespace first
        window = WS_RE.sub("", text[max(0, m.start() - 150) : m.end() + 300])
        last_court = ""
        prev_end = 0
        for c in CASE_NO_RE.finditer(window):
            courts = COURT_RE.findall(window[prev_end : c.start()])
            if courts:
                found = courts[-1]
                if found in ("同院", "同法院"):
                    pass  # keep last_court
                else:
                    last_court = found
            prev_end = c.end()

            word = c.group(2)
            ref_type = "prosecutor" if PROSECUTOR_WORD_RE.search(word) else "court"
            refs.append(
                (
                    last_court if ref_type == "court" else "",
                    to_arabic(c.group(1)),
                    word,
                    to_arabic(c.group(3)),
                    ref_type,
                )
            )
    return list(dict.fromkeys(refs))


def header_court_name(text: str) -> str:
    match = HEADER_COURT_RE.match(WS_RE.sub("", text[:80]))
    return match.group(1) if match else ""


def process_file(path: str) -> tuple[list[dict], list[dict], dict]:
    key = os.environ["NAME_HASH_KEY"].encode("utf-8")
    df = pd.read_parquet(path, columns=["jid", "full_text"])
    defendants, priors = [], []
    court_names: dict[tuple[str, str], int] = {}

    for jid, text in zip(df["jid"], df["full_text"]):
        if not isinstance(text, str):
            continue
        court, year, date = parse_jid(jid)
        dtype = doc_type(text)

        name = header_court_name(text)
        if name:
            court_names[(name, court)] = court_names.get((name, court), 0) + 1

        for person in extract_names(text):
            defendants.append(
                {
                    "jid": jid,
                    "court_code": court,
                    "year": year,
                    "judgment_date": date,
                    "doc_type": dtype,
                    "name_hash": hash_name(person, key),
                }
            )
        for court_text, y, word, no, ref_type in extract_prior_refs(text):
            priors.append(
                {
                    "jid": jid,
                    "cited_court": court_text,
                    "cited_year": y,
                    "cited_word": word,
                    "cited_no": no,
                    "ref_type": ref_type,
                }
            )

    return defendants, priors, court_names


def build_name_freq(defendants_file: Path, output_file: Path):
    df = pd.read_parquet(
        defendants_file, columns=["jid", "name_hash", "court_code", "year"]
    )
    freq = (
        df.groupby("name_hash")
        .agg(
            n_docs=("jid", "nunique"),
            n_courts=("court_code", "nunique"),
            first_year=("year", "min"),
            last_year=("year", "max"),
        )
        .reset_index()
    )
    freq.to_parquet(output_file, index=False)
    return freq


def main():
    if not os.environ.get("NAME_HASH_KEY"):
        sys.exit("NAME_HASH_KEY is not set. Add it to .env (see .env.example).")

    files = sorted(glob.glob(f"{PARQUET_DIR}/*_batch*.parquet"))
    if len(sys.argv) > 1:
        files = files[: int(sys.argv[1])]
        print(f"⚠️  Test mode: first {len(files)} files")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    defendants_file = OUTPUT_DIR / "defendants.parquet"
    priors_file = OUTPUT_DIR / "prior_case_refs.parquet"
    freq_file = OUTPUT_DIR / "name_freq.parquet"
    courts_file = OUTPUT_DIR / "court_names.parquet"

    start = time.time()
    n_def = n_prior = 0
    court_counts: dict[tuple[str, str], int] = {}
    workers = max(1, (os.cpu_count() or 2) - 1)

    with (
        pq.ParquetWriter(defendants_file, DEFENDANT_SCHEMA) as def_writer,
        pq.ParquetWriter(priors_file, PRIOR_SCHEMA) as prior_writer,
        Pool(workers) as pool,
    ):
        for defendants, priors, courts in tqdm(
            pool.imap_unordered(process_file, files, chunksize=4),
            total=len(files),
            desc="Indexing",
        ):
            if defendants:
                def_writer.write_table(
                    pa.Table.from_pylist(defendants, schema=DEFENDANT_SCHEMA)
                )
                n_def += len(defendants)
            if priors:
                prior_writer.write_table(
                    pa.Table.from_pylist(priors, schema=PRIOR_SCHEMA)
                )
                n_prior += len(priors)
            for k, v in courts.items():
                court_counts[k] = court_counts.get(k, 0) + v

    freq = build_name_freq(defendants_file, freq_file)

    court_df = pd.DataFrame(
        [(name, code, n) for (name, code), n in court_counts.items()],
        columns=["court_name", "court_code", "n_docs"],
    ).sort_values("n_docs", ascending=False)
    court_df.to_parquet(courts_file, index=False)

    print(f"\nDefendant rows: {n_def:,} → {defendants_file}")
    print(f"Distinct name hashes: {len(freq):,} → {freq_file}")
    print(f"Prior case references: {n_prior:,} → {priors_file}")
    print(f"Court name/code pairs: {len(court_df):,} → {courts_file}")

    buckets = pd.cut(
        freq["n_docs"],
        [0, 1, 2, 5, 10, 50, float("inf")],
        labels=["1", "2", "3-5", "6-10", "11-50", "50+"],
    )
    print("\nNames by number of documents they appear in:")
    print(buckets.value_counts().sort_index().to_string())
    print(f"\nElapsed: {(time.time() - start) / 60:.1f} min")


if __name__ == "__main__":
    main()
