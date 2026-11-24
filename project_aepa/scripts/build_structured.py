"""
Structure all judgments into sections (see judgment_parser.py) and report parse coverage.

Output: one parquet per input batch in data/structured/ (resumable: existing outputs are
skipped). Each row stores section offsets into full_text, not the text itself:
    jid, status, doc_kind, court_name, facts_heading, reasons_as_facts, attach_type,
    <section>_start / <section>_end for header, main, facts, reasons, combined,
        statutes, attach, attach_facts  (-1 when missing)
    main_guilty / acquitted / not_accepted / exempted / dismissed / revoked
    laws               list of law names in the statutes appendix and final clause
    defendant_hashes   HMAC hashes of full defendant names (NAME_HASH_KEY)
    n_defendants_masked

Test mode parses n files spread evenly over all years into data/structured_test/
(always overwritten) and writes a readable sample for manual checking.

Usage:
    python scripts/build_structured.py --test 50     # test run + report + samples
    python scripts/build_structured.py               # full run (resumable) + report
    python scripts/build_structured.py --force       # full run, re-parse existing outputs
    python scripts/build_structured.py --report      # report only
"""

import glob
import hashlib
import hmac
import os
import random
import sys
from multiprocessing import Pool
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from judgment_parser import NAME_RE, SECTIONS, VERDICT_PATTERNS, parse
from resource_monitor import start_memory_guard
from tqdm import tqdm

load_dotenv()

PARQUET_DIR = "data/processed"
FULL_DIR = Path("data/structured")
TEST_DIR = Path("data/structured_test")
FORCE = "--force" in sys.argv  # re-parse everything after parser changes

SCHEMA = pa.schema(
    [
        ("jid", pa.string()),
        ("status", pa.string()),
        ("doc_kind", pa.string()),
        ("court_name", pa.string()),
        ("facts_heading", pa.string()),
        ("reasons_as_facts", pa.bool_()),
        ("attach_type", pa.string()),
    ]
    + [(f"{s}_{p}", pa.int32()) for s in SECTIONS for p in ("start", "end")]
    + [(f"main_{k}", pa.bool_()) for k in VERDICT_PATTERNS]
    + [
        ("laws", pa.list_(pa.string())),
        ("cites_animal_protection_act", pa.bool_()),
        ("defendant_hashes", pa.list_(pa.string())),
        ("n_defendants_masked", pa.int32()),
    ]
)


def to_row(jid: str, text: str, key: bytes) -> dict:
    r = parse(text)
    row = {
        k: r[k]
        for k in [
            "status",
            "doc_kind",
            "court_name",
            "facts_heading",
            "reasons_as_facts",
            "attach_type",
            "laws",
            "cites_animal_protection_act",
        ]
    }
    row["jid"] = jid
    for s in SECTIONS:
        row[f"{s}_start"], row[f"{s}_end"] = r[f"{s}_span"]
    for k in VERDICT_PATTERNS:
        row[f"main_{k}"] = r[f"main_{k}"]
    named = [name for _, name in r["defendants"] if NAME_RE.match(name)]
    row["defendant_hashes"] = list(
        dict.fromkeys(
            hmac.new(key, n.encode("utf-8"), hashlib.sha256).hexdigest()[:20]
            for n in named
        )
    )
    row["n_defendants_masked"] = len(r["defendants"]) - len(named)
    return row


def process_file(args: tuple[str, str]) -> int:
    path, out_dir = args
    out = Path(out_dir) / Path(path).name
    if out_dir == str(FULL_DIR) and out.exists() and not FORCE:
        return 0
    key = os.environ["NAME_HASH_KEY"].encode("utf-8")
    df = pd.read_parquet(path, columns=["jid", "full_text"])
    empty = {f.name: None for f in SCHEMA}
    rows = []
    for jid, text in zip(df["jid"], df["full_text"]):
        if not isinstance(text, str):
            rows.append({**empty, "jid": jid, "status": "no_text"})
            continue
        try:
            rows.append(to_row(jid, text, key))
        except Exception as e:  # one odd document must not stop the whole run
            rows.append(
                {**empty, "jid": jid, "status": f"parse_error:{type(e).__name__}"}
            )
    tmp = out.with_suffix(".tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), tmp)
    tmp.rename(out)  # atomic: an interrupted run never leaves a half-written file
    return len(rows)


def report(out_dir: Path):
    cols = [
        "jid",
        "status",
        "doc_kind",
        "facts_start",
        "combined_start",
        "attach_facts_start",
        "reasons_start",
        "statutes_start",
        "main_guilty",
        "laws",
        "defendant_hashes",
        "n_defendants_masked",
        "facts_heading",
        "reasons_as_facts",
        "cites_animal_protection_act",
    ]
    df = pd.concat(
        [pd.read_parquet(f, columns=cols) for f in sorted(out_dir.glob("*.parquet"))],
        ignore_index=True,
    )
    parts = df["jid"].str.split(",")
    df["court_level"] = (
        parts.str[0]
        .str[2]
        .map({"D": "district", "E": "summary_court", "H": "high", "S": "supreme"})
        .fillna("other")
    )
    year = pd.to_numeric(parts.str[4].str[:4], errors="coerce")
    df["period"] = pd.cut(
        year,
        [0, 1999, 2005, 2010, 2015, 2020, 2030],
        labels=["<2000", "2000-05", "2006-10", "2011-15", "2016-20", "2021+"],
    )
    df["has_facts_any"] = (
        (df["facts_start"] >= 0)
        | (df["combined_start"] >= 0)
        | (df["attach_facts_start"] >= 0)
    )
    df["has_laws"] = df["laws"].map(lambda x: x is not None and len(x) > 0)
    df["has_named_def"] = df["defendant_hashes"].map(
        lambda x: x is not None and len(x) > 0
    )
    df["has_any_def"] = df["has_named_def"] | (df["n_defendants_masked"].fillna(0) > 0)
    df["animal_act"] = df["cites_animal_protection_act"].fillna(False).astype(bool)

    print(f"\nDocuments: {len(df):,}")
    print("\nStatus:")
    print(df["status"].value_counts().to_string())
    print("\nDocument kind:")
    print(df["doc_kind"].value_counts().to_string())

    def coverage(group_cols: list[str], subset: pd.DataFrame, label: str):
        flags = {
            "has_main": subset["status"] != "no_main",
            "facts_any": subset["has_facts_any"],
            "facts": subset["facts_start"] >= 0,
            "combined": subset["combined_start"] >= 0,
            "attach_facts": subset["attach_facts_start"] >= 0,
            "reasons": subset["reasons_start"] >= 0,
            "statutes": subset["statutes_start"] >= 0,
            "laws": subset["has_laws"],
            "any_def": subset["has_any_def"],
            "named_def": subset["has_named_def"],
        }
        t = (
            pd.DataFrame(flags)
            .groupby([subset[c] for c in group_cols], observed=True)
            .mean()
            * 100
        )
        t.insert(0, "n", subset.groupby(group_cols, observed=True).size())
        print(f"\n{label} (% of documents):")
        with pd.option_context(
            "display.width", 220, "display.float_format", "{:.1f}".format
        ):
            print(t.to_string())

    judgments = df[df["doc_kind"].isin(["judgment", "summary_judgment"])]
    coverage(["period"], judgments, "Judgments by period")
    coverage(["court_level"], judgments, "Judgments by court level")
    coverage(["doc_kind"], df, "All documents by kind")

    print("\nFacts headings used (judgments):")
    print(
        judgments["facts_heading"]
        .replace("", "(none)")
        .value_counts()
        .head(15)
        .to_string()
    )
    print(
        f"Old format with 理由 used as facts: {judgments['reasons_as_facts'].sum():,}"
    )

    print(f"\nDocuments citing 動物保護法 in laws: {df['animal_act'].sum():,}")
    print(df.loc[df["animal_act"], "doc_kind"].value_counts().to_string())

    missing = judgments[
        ~judgments["has_facts_any"] | (judgments["status"] == "no_main")
    ]
    sample = missing.sample(min(30, len(missing)), random_state=0)["jid"]
    path = out_dir / "_missing_facts_sample.csv"
    sample.to_csv(path, index=False)
    print(f"\nSample of judgments without facts or 主文 → {path}")


def write_samples(files: list[str], out_dir: Path, n: int = 40):
    """Readable dump of parsed sections for manual checking (local only, contains text)."""
    from judgment_parser import parse as parse_doc

    random.seed(0)
    picked = []
    for f in random.sample(files, min(10, len(files))):
        df = pd.read_parquet(f, columns=["jid", "full_text"]).dropna()
        picked += df.sample(min(n // 10, len(df)), random_state=0).values.tolist()
    lines = []
    for jid, text in picked:
        r = parse_doc(text)
        lines.append(
            f"===== {jid} | {r['status']} | {r['doc_kind']} | facts_heading={r['facts_heading']} "
            f"| attach={r['attach_type']} | defendants={len(r['defendants'])} | laws={r['laws']}"
        )
        for s in SECTIONS:
            a, b = r[f"{s}_span"]
            if a >= 0 and s not in ("header", "attach"):
                snippet = " ".join(text[a:b].split())
                lines.append(
                    f"  [{s}] ({b - a} chars) {snippet[:80]} … {snippet[-40:]}"
                )
    path = out_dir / "_samples.txt"
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Readable samples → {path}")


def main():
    args = sys.argv[1:]
    if not os.environ.get("NAME_HASH_KEY"):
        sys.exit("NAME_HASH_KEY is not set. Add it to .env (see .env.example).")

    start_memory_guard(10)
    if args[:1] == ["--report"]:
        report(FULL_DIR)
        return

    files = sorted(glob.glob(f"{PARQUET_DIR}/*_batch*.parquet"))
    out_dir = FULL_DIR
    if args[:1] == ["--test"]:
        n = int(args[1]) if len(args) > 1 else 50
        step = max(1, len(files) // n)
        files = files[::step][:n]
        out_dir = TEST_DIR
        for old in out_dir.glob("*.parquet"):
            old.unlink()
        print(f"⚠️  Test mode: {len(files)} files spread over all years → {out_dir}")

    out_dir.mkdir(parents=True, exist_ok=True)
    workers = max(1, (os.cpu_count() or 2) - 1)
    with Pool(workers) as pool:
        n_docs = sum(
            tqdm(
                pool.imap_unordered(process_file, [(f, str(out_dir)) for f in files]),
                total=len(files),
                desc="Structuring",
            )
        )
    print(f"Parsed documents this run: {n_docs:,}")

    report(out_dir)
    if out_dir == TEST_DIR:
        write_samples(files, out_dir)


if __name__ == "__main__":
    main()
