"""
Stage 3: LLM Verification (ULTRA Optimized)

ALL optimizations applied:
1. Structured output (JSON Schema)
2. Few-shot examples (improve accuracy)
3. Minimal text (500 chars)
4. Aggressive parameters
5. Parallel processing (3-5x speedup!)
6. Batch API calls

Expected: 3-5 sec/case → 4-8 hours for 17K cases
"""

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import ollama
import pandas as pd
from tqdm import tqdm


# ============================================================
# Configuration
# ============================================================

OLLAMA_MODEL = "qwen2.5:3b"
PARALLEL_WORKERS = 4  # Process 4 cases simultaneously

# Animal keywords
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
    "小狗",
    "小貓",
]

# JSON Schema for structured output
OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "is_abuse": {
            "type": "boolean",
            "description": "True if animal abuse, False if false positive",
        },
        "reason": {
            "type": "string",
            "description": "Brief reason (max 20 chars)",
            "maxLength": 20,
        },
    },
    "required": ["is_abuse", "reason"],
}

# Few-shot prompt with CLEAR logic
VERIFICATION_PROMPT_FEWSHOT = """你是動物保護法專家。判斷此案是否為「對動物施加暴力、虐待、遺棄」。

案由：{title}
內容節錄：{text}

---
範例 1（真的虐待）:
內容：被告持鐵鎚毆打鄰居飼養的犬隻致死
答：{{"is_abuse": true, "reason": "毆打犬致死"}}

範例 2（人名綽號，非動物）:
內容：被告與阿狗發生口角後動手
答：{{"is_abuse": false, "reason": "阿狗是人名"}}

範例 3（動物是加害者，非虐待）:
內容：告訴人遭狗咬傷
答：{{"is_abuse": false, "reason": "動物咬人"}}

範例 4（比喻用語，非真實動物）:
內容：你要乖得像狗一樣
答：{{"is_abuse": false, "reason": "比喻用語"}}

範例 5（背景資訊，無虐待行為）:
內容：被害人生前有養一隻狗
答：{{"is_abuse": false, "reason": "背景資訊"}}

---
現在判斷：
案由：{title}
內容：{text}

規則：
- is_abuse: true（真的虐待動物）或 false（不是虐待）
- reason: 如果是虐待，寫虐待方式（如「毆打」「遺棄」）；如果不是，寫排除原因（如「人名」「比喻」「動物咬人」「背景」）
- reason 限制 10 字內

答（只回答 JSON，不要其他文字）："""


def extract_minimal_context(full_text: str, max_chars: int = 500) -> str:
    """Extract minimal relevant context (ultra short)"""
    if not full_text:
        return ""

    # Find first animal keyword match
    for keyword in ANIMAL_KEYWORDS:
        match = re.search(keyword, full_text)
        if match:
            pos = match.start()
            # Take small context around first match
            start = max(0, pos - 150)
            end = min(len(full_text), pos + 350)
            snippet = full_text[start:end]

            # Clean up
            snippet = re.sub(r"\s+", " ", snippet).strip()

            if len(snippet) > max_chars:
                snippet = snippet[:max_chars]

            return snippet

    # Fallback: beginning
    return re.sub(r"\s+", " ", full_text[:max_chars]).strip()


def get_case_fulltext(jid: str, parquet_dir: str = "data/processed") -> str | None:
    """Get full judgment text"""
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


def verify_single_case(case: dict, model: str = OLLAMA_MODEL) -> dict:
    """
    Verify one case with ALL optimizations

    Returns:
        Dict with jid, is_abuse, reason
    """

    # 1. Extract minimal text
    full_text = case.get("full_text", "")
    snippet = extract_minimal_context(full_text, max_chars=500)

    # 2. Few-shot prompt
    prompt = VERIFICATION_PROMPT_FEWSHOT.format(
        title=case.get("title", "")[:30],  # Limit title too
        text=snippet,
    )

    # 3. Call with structured output + aggressive params
    try:
        response = ollama.chat(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            format=OUTPUT_SCHEMA,  # Structured output!
            options={
                "temperature": 0,
                "num_predict": 50,  # Short JSON output
                "top_p": 0.9,
            },
        )

        content = response["message"]["content"].strip()

        # Parse JSON
        # Remove markdown code blocks if present
        if "```" in content:
            content = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", content, re.DOTALL)
            if content:
                content = content.group(1)

        result = json.loads(content)

        return {
            "jid": case["jid"],
            "is_abuse": result.get("is_abuse"),
            "reason": result.get("reason", "")[:30],
        }

    except json.JSONDecodeError:
        # Fallback: parse from raw text
        content_lower = content.lower() if "content" in locals() else ""
        is_abuse = None
        if "true" in content_lower[:30]:
            is_abuse = True
        elif "false" in content_lower[:30]:
            is_abuse = False

        return {
            "jid": case["jid"],
            "is_abuse": is_abuse,
            "reason": "PARSE_ERROR",
        }

    except Exception:
        return {
            "jid": case["jid"],
            "is_abuse": None,
            "reason": "ERROR",
        }


def verify_batch_parallel(
    cases: list, model: str = OLLAMA_MODEL, workers: int = PARALLEL_WORKERS
) -> list:
    """
    Verify multiple cases in parallel (KEY OPTIMIZATION!)

    Args:
        cases: List of case dicts
        model: Model name
        workers: Number of parallel workers

    Returns:
        List of results
    """

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(verify_single_case, case, model) for case in cases]
        results = [f.result() for f in futures]

    return results


def verify_candidates_ultra(
    tagged_csv: str,
    output_dir: str = "data/verified",
    model: str = OLLAMA_MODEL,
    workers: int = PARALLEL_WORKERS,
    limit: int = None,
) -> str:
    """
    ULTRA fast verification with ALL optimizations
    """

    print("=" * 60)
    print("Stage 3: LLM Verification (ULTRA Mode)")
    print("=" * 60)
    print(f"\nModel: {model}")
    print(f"Parallel workers: {workers}")
    print("\nOptimizations:")
    print("  ✓ Structured output (JSON Schema)")
    print("  ✓ Few-shot examples")
    print("  ✓ Minimal text (500 chars)")
    print("  ✓ Aggressive parameters")
    print("  ✓ PARALLEL PROCESSING (KEY!)")

    # Test
    print("\n🧪 Testing...")
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
        print(f"\n⚠️  Test mode: {limit} cases")

    print(f"\n📊 Total: {len(df_candidates):,} cases")

    # Prepare output
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_path / f"verified_ultra_{timestamp}.csv"

    print(f"\n🔄 Processing with {workers} parallel workers...")
    print(f"💾 Output: {output_file}")

    # Process in batches (for parallel execution)
    results = []
    start_time = time.time()

    batch_size = workers * 2  # Process 2x workers at a time

    with tqdm(total=len(df_candidates), desc="Verifying") as pbar:
        for i in range(0, len(df_candidates), batch_size):
            batch_df = df_candidates.iloc[i : i + batch_size]

            # Prepare batch cases
            batch_cases = []
            for _, row in batch_df.iterrows():
                full_text = get_case_fulltext(row["jid"])
                if full_text:
                    batch_cases.append(
                        {
                            "jid": row["jid"],
                            "title": row["title"],
                            "full_text": full_text,
                        }
                    )
                else:
                    results.append(
                        {
                            "jid": row["jid"],
                            "is_abuse": None,
                            "reason": "NO_TEXT",
                        }
                    )

            # Process batch in parallel
            if batch_cases:
                batch_results = verify_batch_parallel(batch_cases, model, workers)
                results.extend(batch_results)

            pbar.update(len(batch_df))

            # Update stats every few batches
            if len(results) % 50 == 0:
                abuse_count = sum(1 for r in results if r.get("is_abuse") is True)
                elapsed = time.time() - start_time
                rate = len(results) / elapsed if elapsed > 0 else 0
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

            # Save progress every 500 cases
            if len(results) % 500 == 0:
                df_temp = pd.DataFrame(results)
                df_temp.to_csv(output_file, index=False, encoding="utf-8")

    # Final save
    df_results = pd.DataFrame(results)
    df_final = df_candidates.merge(df_results, on="jid", how="left")
    df_final.to_csv(output_file, index=False, encoding="utf-8")

    # Summary
    elapsed = time.time() - start_time
    print(f"\n\n{'=' * 60}")
    print("ULTRA Mode Complete!")
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
        print(
            f"\n📈 Estimate for 17,267 cases: {full_time:.1f} hours ({full_time * 60:.0f} min)"
        )

    return output_file


if __name__ == "__main__":
    import sys

    TAGGED_CSV = "data/filtered/criminal_judgments_tagged_20261002_103027_final.csv"
    MODEL = sys.argv[1] if len(sys.argv) > 1 else "qwen2.5:3b"
    LIMIT = int(sys.argv[2]) if len(sys.argv) > 2 else None
    WORKERS = int(sys.argv[3]) if len(sys.argv) > 3 else PARALLEL_WORKERS

    if LIMIT:
        print(f"\n⚠️  TEST MODE: {LIMIT} cases\n")

    output = verify_candidates_ultra(
        TAGGED_CSV,
        model=MODEL,
        workers=WORKERS,
        limit=LIMIT,
    )

    if output:
        print("\n" + "=" * 60)
        print("🚀 ULTRA optimization complete!")
        print("=" * 60)
