"""
Stage 3: LLM Verification and Classification

Uses local LLM (Ollama) to verify and classify candidate animal abuse cases.

Strategy:
- Read 17,267 candidate cases from Stage 1 + Stage 2
- For each case, read full judgment text from Parquet
- Use Qwen2.5 to judge:
  * Is it really animal abuse? (not false positive)
  * What type of abuse?
  * Confidence level
- Output: Verified and classified cases

Performance estimate on M3 Pro:
- Qwen2.5:7b at ~20 tok/s: ~2-3 hours for 17K cases
- Qwen2.5:3b at ~50 tok/s: ~1-1.5 hours for 17K cases
"""

import json
import time
from pathlib import Path

import ollama
import pandas as pd
from tqdm import tqdm


# ============================================================
# Configuration
# ============================================================

OLLAMA_MODEL = "qwen2.5:7b"  # or "qwen2.5:3b" for faster processing

# LLM prompt template
VERIFICATION_PROMPT = """你是一個法律文件分析專家，專門判斷刑事判決是否涉及動物虐待。

判決摘要：
案由：{title}
年份：{year}

判決內容節錄：
{text_snippet}

請判斷這個案件是否為動物虐待案件，並回答以下問題：

1. 是否為動物虐待案件？（是/否）
2. 信心程度？（高/中/低）
3. 如果是虐待，類型為何？（直接暴力/遺棄/威脅/其他）
4. 如果不是虐待，原因為何？（綽號/地名/動物是加害者/交通事故/其他）

請用以下 JSON 格式回答（不要有其他文字）：
{{
  "is_abuse": true,
  "confidence": "high",
  "abuse_type": "直接暴力",
  "false_positive_reason": ""
}}
"""


def get_case_fulltext(jid: str, parquet_dir: str = "data/processed") -> str | None:
    """Get full judgment text for a given JID from Parquet files"""
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


def verify_with_llm(
    case: dict, model: str = OLLAMA_MODEL, text_limit: int = 3000
) -> dict:
    """
    Verify a single case with LLM

    Args:
        case: Dict with keys: jid, title, year, full_text
        model: Ollama model name
        text_limit: Max characters of text to send to LLM

    Returns:
        Dict with verification results
    """

    # Prepare text snippet (limit to avoid token overflow)
    full_text = case.get("full_text", "")
    if len(full_text) > text_limit:
        # Take beginning + middle + end
        snippet = (
            full_text[: text_limit // 3]
            + "\n\n[...中間省略...]\n\n"
            + full_text[len(full_text) // 2 : len(full_text) // 2 + text_limit // 3]
            + "\n\n[...中間省略...]\n\n"
            + full_text[-text_limit // 3 :]
        )
    else:
        snippet = full_text

    # Prepare prompt
    prompt = VERIFICATION_PROMPT.format(
        title=case.get("title", ""),
        year=case.get("year", ""),
        text_snippet=snippet,
    )

    # Call LLM
    try:
        response = ollama.chat(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            options={
                "temperature": 0.1,  # Low temperature for consistent results
                "num_predict": 200,  # Limit output length (we only need JSON)
            },
        )

        # Parse response
        content = response["message"]["content"].strip()

        # Try to extract JSON (sometimes LLM adds markdown code blocks)
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()

        result = json.loads(content)

        # Add metadata
        result["jid"] = case["jid"]
        result["verification_model"] = model
        result["verification_timestamp"] = pd.Timestamp.now().isoformat()

        return result

    except json.JSONDecodeError as e:
        print(f"\n⚠️  JSON parse error for {case['jid']}: {e}")
        print(f"Response: {content[:200]}...")
        return {
            "jid": case["jid"],
            "is_abuse": None,
            "confidence": "low",
            "abuse_type": "",
            "false_positive_reason": "LLM_ERROR",
            "error": str(e),
        }

    except Exception as e:
        print(f"\n❌ Error verifying {case['jid']}: {e}")
        return {
            "jid": case["jid"],
            "is_abuse": None,
            "confidence": "low",
            "abuse_type": "",
            "false_positive_reason": "ERROR",
            "error": str(e),
        }


def verify_candidates(
    tagged_csv: str,
    output_dir: str = "data/verified",
    model: str = OLLAMA_MODEL,
    limit: int = None,
    batch_size: int = 100,
) -> str:
    """
    Verify all candidate cases with LLM

    Args:
        tagged_csv: Path to final tagged CSV
        output_dir: Output directory
        model: Ollama model name
        limit: Limit number of cases to process (for testing)
        batch_size: Save results every N cases

    Returns:
        Path to verified results CSV
    """

    print("=" * 60)
    print("Stage 3: LLM Verification and Classification")
    print("=" * 60)
    print(f"\nModel: {model}")

    # Test Ollama connection
    print("\n🧪 Testing Ollama connection...")
    try:
        response = ollama.chat(
            model=model,
            messages=[{"role": "user", "content": "請用中文說「你好」"}],
        )
        print(f"✓ Ollama OK: {response['message']['content']}")
    except Exception as e:
        print(f"❌ Ollama connection failed: {e}")
        print("\n請確認：")
        print("1. ollama serve 是否在執行")
        print(f"2. 模型是否已下載：ollama pull {model}")
        return None

    # Load candidate cases
    print(f"\n📂 Loading candidates from: {tagged_csv}")

    candidates = []
    for chunk in pd.read_csv(tagged_csv, chunksize=100000, low_memory=False):
        # Filter: Stage 1 or Stage 2 matches
        mask = (chunk["animal_case_stage1"].eq(True)) | (
            chunk["animal_case_stage2"].eq(True)
        )
        matched = chunk[mask].copy()

        if len(matched) > 0:
            candidates.append(matched[["jid", "title", "year", "court_code"]])

    if not candidates:
        print("❌ No candidates found!")
        return None

    df_candidates = pd.concat(candidates, ignore_index=True)

    if limit:
        df_candidates = df_candidates.head(limit)
        print(f"\n⚠️  Limited to first {limit} cases for testing")

    print(f"\n📊 Total candidates: {len(df_candidates):,}")

    # Prepare output
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_path / f"verified_animal_abuse_{timestamp}.csv"
    temp_file = output_path / f"verified_animal_abuse_{timestamp}_temp.jsonl"

    # Process each case
    print(f"\n🔄 Processing with {model}...")
    print(f"💾 Results will be saved to: {output_file}")
    print(f"💾 Progress saved to: {temp_file}")

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
                    "false_positive_reason": "NO_FULLTEXT",
                }
            else:
                # Verify with LLM
                case = {
                    "jid": row["jid"],
                    "title": row["title"],
                    "year": row["year"],
                    "full_text": full_text,
                }
                result = verify_with_llm(case, model=model)

            results.append(result)

            # Save progress every batch_size cases
            if len(results) % batch_size == 0:
                # Save to temp JSONL file
                with open(temp_file, "a", encoding="utf-8") as f:
                    for r in results[-batch_size:]:
                        f.write(json.dumps(r, ensure_ascii=False) + "\n")

            pbar.update(1)

            # Show stats every 100 cases
            if len(results) % 100 == 0:
                abuse_count = sum(1 for r in results if r.get("is_abuse") is True)
                elapsed = time.time() - start_time
                rate = len(results) / elapsed
                remaining = (len(df_candidates) - len(results)) / rate
                pbar.set_postfix(
                    {
                        "abuse": abuse_count,
                        "rate": f"{rate:.1f}/s",
                        "ETA": f"{remaining / 60:.0f}m",
                    }
                )

    # Save final results
    df_results = pd.DataFrame(results)

    # Merge with original candidate data
    df_final = df_candidates.merge(df_results, on="jid", how="left")

    df_final.to_csv(output_file, index=False, encoding="utf-8")

    # Clean up temp file
    if temp_file.exists():
        temp_file.unlink()

    # Print summary
    elapsed = time.time() - start_time
    print(f"\n\n{'=' * 60}")
    print("Stage 3 Complete")
    print("=" * 60)
    print(f"\nTotal processed: {len(results):,}")
    print(f"Time elapsed: {elapsed / 60:.1f} minutes")
    print(f"Average rate: {len(results) / elapsed:.1f} cases/sec")

    # Count by result
    abuse_count = sum(1 for r in results if r.get("is_abuse") is True)
    not_abuse_count = sum(1 for r in results if r.get("is_abuse") is False)
    error_count = sum(1 for r in results if r.get("is_abuse") is None)

    print("\n📊 Results:")
    print(f"  ✅ Confirmed abuse: {abuse_count:,}")
    print(f"  ❌ Not abuse (false positive): {not_abuse_count:,}")
    print(f"  ⚠️  Errors: {error_count:,}")
    print(f"  📈 True positive rate: {abuse_count / len(results) * 100:.1f}%")

    # Confidence distribution
    high_conf = sum(1 for r in results if r.get("confidence") == "high")
    med_conf = sum(1 for r in results if r.get("confidence") == "medium")
    low_conf = sum(1 for r in results if r.get("confidence") == "low")

    print("\n🎯 Confidence:")
    print(f"  High: {high_conf:,}")
    print(f"  Medium: {med_conf:,}")
    print(f"  Low: {low_conf:,}")

    print(f"\n✓ Saved to: {output_file}")

    return output_file


if __name__ == "__main__":
    import sys

    # Configuration
    TAGGED_CSV = "data/filtered/criminal_judgments_tagged_20261002_103027_final.csv"
    MODEL = sys.argv[1] if len(sys.argv) > 1 else "qwen2.5:7b"
    LIMIT = int(sys.argv[2]) if len(sys.argv) > 2 else None

    if LIMIT:
        print(f"\n⚠️  TEST MODE: Processing only {LIMIT} cases\n")

    # Execute verification
    output_file = verify_candidates(
        TAGGED_CSV,
        model=MODEL,
        limit=LIMIT,
    )

    if output_file:
        print("\n" + "=" * 60)
        print("Next steps:")
        print("=" * 60)
        print("1. Review verified results")
        print("2. Analyze abuse types and patterns")
        print("3. Begin prior conviction analysis")
