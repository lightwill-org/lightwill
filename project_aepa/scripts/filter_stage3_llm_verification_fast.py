"""
Stage 3: LLM Verification (Optimized Version)

Optimizations:
- Shorter text snippets (800 chars vs 3000)
- Simpler prompt (yes/no instead of complex JSON)
- Faster LLM parameters
- Extract only relevant paragraphs around animal keywords

Expected speed: 5-10 sec/case (3-5x faster)
"""

import re
import time
from pathlib import Path

import ollama
import pandas as pd
from tqdm import tqdm


# ============================================================
# Configuration
# ============================================================

OLLAMA_MODEL = "qwen2.5:3b"

# Animal keywords for context extraction
ANIMAL_KEYWORDS = [
    "動物",
    "寵物",
    "犬",
    "犬隻",
    "狗",
    "貓",
    "貓隻",
    "流浪犬",
    "流浪貓",
    "流浪狗",
    "小狗",
    "小貓",
]

# Simple prompt (much faster)
VERIFICATION_PROMPT = """判決案由：{title}

判決內容節錄：
{text_snippet}

問題：這個案件是否為「對動物施加暴力、虐待、遺棄」的動物虐待案件？

注意排除：
- 綽號/人名（例如「阿狗」是人名）
- 地名（例如「貓貍影城」）
- 動物是加害者（例如「遭狗咬傷」）
- 交通事故（過失撞到動物）

請只回答一個字：是 或 否"""


def extract_animal_context(full_text: str, max_chars: int = 800) -> str:
    """
    Extract relevant paragraphs containing animal keywords
    Much faster than sending full 3000 char text
    """
    if not full_text:
        return ""

    # Find all matches of animal keywords
    matches = []
    for keyword in ANIMAL_KEYWORDS:
        for match in re.finditer(keyword, full_text):
            pos = match.start()
            # Extract context around keyword (±200 chars)
            start = max(0, pos - 200)
            end = min(len(full_text), pos + 200)
            matches.append((start, end, full_text[start:end]))

    if not matches:
        # Fallback: take beginning
        return full_text[:max_chars]

    # Merge overlapping contexts
    matches.sort()
    merged = []
    current_start, current_end, current_text = matches[0]

    for start, end, text in matches[1:]:
        if start <= current_end + 50:  # Merge if close
            current_end = end
            current_text = full_text[current_start:current_end]
        else:
            merged.append(current_text)
            current_start, current_end, current_text = start, end, text

    merged.append(current_text)

    # Join and limit
    result = "...\n".join(merged)
    if len(result) > max_chars:
        result = result[:max_chars] + "..."

    return result


def get_case_fulltext(jid: str, parquet_dir: str = "data/processed") -> str | None:
    """Get full judgment text for a given JID"""
    import glob

    parquet_files = glob.glob(f"{parquet_dir}/*_batch*.parquet")

    for pf in parquet_files:
        try:
            df = pd.read_parquet(pf, columns=["jid", "full_text"])
            matched = df[df["jid"] == jid]
            if len(matched) > 0:
                return matched.iloc[0]["full_text"]
        except Exception:
            continue

    return None


def verify_with_llm_fast(case: dict, model: str = OLLAMA_MODEL) -> dict:
    """
    Fast verification with simplified prompt

    Returns:
        Dict with: jid, is_abuse (True/False/None), raw_response
    """

    # Extract relevant context only (much shorter)
    full_text = case.get("full_text", "")
    snippet = extract_animal_context(full_text, max_chars=800)

    # Simple prompt
    prompt = VERIFICATION_PROMPT.format(
        title=case.get("title", ""),
        text_snippet=snippet,
    )

    # Call LLM with optimized parameters
    try:
        response = ollama.chat(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            options={
                "temperature": 0.1,
                "num_predict": 10,  # Only need 1-2 tokens (是/否)
                "top_p": 0.9,
            },
        )

        content = response["message"]["content"].strip()

        # Parse simple yes/no
        is_abuse = None
        if "是" in content[:5]:  # Check first 5 chars
            is_abuse = True
        elif "否" in content[:5]:
            is_abuse = False

        return {
            "jid": case["jid"],
            "is_abuse": is_abuse,
            "confidence": "medium",  # Always medium for fast mode
            "raw_response": content[:50],  # First 50 chars only
        }

    except Exception as e:
        return {
            "jid": case["jid"],
            "is_abuse": None,
            "confidence": "low",
            "raw_response": f"ERROR: {str(e)[:50]}",
        }


def verify_candidates_fast(
    tagged_csv: str,
    output_dir: str = "data/verified",
    model: str = OLLAMA_MODEL,
    limit: int = None,
    batch_save: int = 100,
) -> str:
    """
    Fast verification of all candidates

    Args:
        tagged_csv: Path to final tagged CSV
        output_dir: Output directory
        model: Ollama model name
        limit: Limit number of cases (for testing)
        batch_save: Save every N cases

    Returns:
        Path to output CSV
    """

    print("=" * 60)
    print("Stage 3: LLM Verification (FAST Mode)")
    print("=" * 60)
    print(f"\nModel: {model}")
    print("Optimizations:")
    print("  ✓ Short text snippets (800 chars)")
    print("  ✓ Simple yes/no prompt")
    print("  ✓ Fast LLM parameters")

    # Test connection
    print("\n🧪 Testing Ollama...")
    try:
        test = ollama.chat(
            model=model,
            messages=[{"role": "user", "content": "說「好」"}],
            options={"num_predict": 5},
        )
        print(f"✓ OK: {test['message']['content']}")
    except Exception as e:
        print(f"❌ Failed: {e}")
        return None

    # Load candidates
    print(f"\n📂 Loading from: {tagged_csv}")

    candidates = []
    for chunk in pd.read_csv(tagged_csv, chunksize=100000, low_memory=False):
        mask = (chunk["animal_case_stage1"].eq(True)) | (
            chunk["animal_case_stage2"].eq(True)
        )
        matched = chunk[mask].copy()
        if len(matched) > 0:
            candidates.append(matched[["jid", "title", "year"]])

    if not candidates:
        print("❌ No candidates")
        return None

    df_candidates = pd.concat(candidates, ignore_index=True)

    if limit:
        df_candidates = df_candidates.head(limit)
        print(f"\n⚠️  Test mode: {limit} cases only")

    print(f"\n📊 Total: {len(df_candidates):,} cases")

    # Prepare output
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_path / f"verified_fast_{timestamp}.csv"

    # Process
    print("\n🔄 Processing...")
    print(f"💾 Output: {output_file}")

    results = []
    start_time = time.time()

    with tqdm(total=len(df_candidates), desc="Verifying") as pbar:
        for idx, row in df_candidates.iterrows():
            # Get full text
            full_text = get_case_fulltext(row["jid"])

            if not full_text:
                result = {
                    "jid": row["jid"],
                    "is_abuse": None,
                    "confidence": "low",
                    "raw_response": "NO_TEXT",
                }
            else:
                # Verify
                case = {
                    "jid": row["jid"],
                    "title": row["title"],
                    "full_text": full_text,
                }
                result = verify_with_llm_fast(case, model=model)

            results.append(result)

            # Save progress
            if len(results) % batch_save == 0:
                df_temp = pd.DataFrame(results)
                df_temp.to_csv(output_file, index=False, encoding="utf-8")

            # Update progress
            if len(results) % 50 == 0:
                abuse_count = sum(1 for r in results if r.get("is_abuse") is True)
                elapsed = time.time() - start_time
                rate = len(results) / elapsed
                remaining = (
                    (len(df_candidates) - len(results)) / rate if rate > 0 else 0
                )

                pbar.set_postfix(
                    {
                        "abuse": abuse_count,
                        "rate": f"{rate:.2f}/s",
                        "ETA": f"{remaining / 60:.0f}m",
                    }
                )

            pbar.update(1)

    # Final save
    df_results = pd.DataFrame(results)
    df_final = df_candidates.merge(df_results, on="jid", how="left")
    df_final.to_csv(output_file, index=False, encoding="utf-8")

    # Summary
    elapsed = time.time() - start_time
    print(f"\n\n{'=' * 60}")
    print("Complete!")
    print("=" * 60)
    print(f"\nProcessed: {len(results):,}")
    print(f"Time: {elapsed / 60:.1f} min ({elapsed:.0f} sec)")
    print(f"Rate: {len(results) / elapsed:.2f} cases/sec")
    print(f"Avg: {elapsed / len(results):.1f} sec/case")

    abuse_count = sum(1 for r in results if r.get("is_abuse") is True)
    not_abuse = sum(1 for r in results if r.get("is_abuse") is False)
    errors = sum(1 for r in results if r.get("is_abuse") is None)

    print("\n📊 Results:")
    print(f"  ✅ Abuse: {abuse_count:,} ({abuse_count / len(results) * 100:.1f}%)")
    print(f"  ❌ Not abuse: {not_abuse:,} ({not_abuse / len(results) * 100:.1f}%)")
    print(f"  ⚠️  Errors: {errors:,}")

    print(f"\n✓ Saved: {output_file}")

    # Estimate for full run
    if limit:
        full_time = (elapsed / len(results)) * 17267 / 3600
        print(f"\n📈 Estimate for 17,267 cases: {full_time:.1f} hours")

    return output_file


if __name__ == "__main__":
    import sys

    TAGGED_CSV = "data/filtered/criminal_judgments_tagged_20261002_103027_final.csv"
    MODEL = sys.argv[1] if len(sys.argv) > 1 else "qwen2.5:3b"
    LIMIT = int(sys.argv[2]) if len(sys.argv) > 2 else None

    if LIMIT:
        print(f"\n⚠️  TEST MODE: {LIMIT} cases\n")

    output = verify_candidates_fast(TAGGED_CSV, model=MODEL, limit=LIMIT)

    if output:
        print("\n" + "=" * 60)
        print("Next: Review results and decide on full run")
        print("=" * 60)
