"""
Stage 3: LLM Verification (vLLM, two-step structured output)

Step 1 (all candidates): did the defendant harm, abandon, or threaten a real animal?
Step 2 (only step-1 yes/uncertain): harm type, animal type, court finding.

Prior animal-related convictions (signal C) are detected separately by regex.
See section 7.3.1 of animal-abuse-crime-linkage-data-plan.md.

Input: data/filtered/stage3_input.parquet (build_stage3_input.py), which holds the
passages around animal mentions in each candidate's facts sections.

Resumable: results go to a fixed file per model and input; documents already labelled
(step 1 parsed) are skipped on the next run, so an interrupted run can be restarted.

Prerequisites:
1. python scripts/build_stage3_input.py [--test]
2. vllm serve <model> --max-model-len 8192 --port 8000

Usage:
    python scripts/filter_stage3_llm_verification_vllm.py <model> [limit|all] [concurrency] [--test]
    python scripts/filter_stage3_llm_verification_vllm.py Qwen/Qwen3-8B-MLX-4bit 50 8
"""

import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests
from resource_monitor import ResourceMonitor
from tqdm import tqdm

# Bump when prompts or schemas change: results of different versions never mix
PROMPT_VERSION = "p2"
VLLM_BASE_URL = "http://localhost:8000/v1"
VLLM_METRICS_URL = "http://localhost:8000/metrics"
INPUT_FILE = "data/filtered/stage3_input{suffix}.parquet"
OUTPUT_DIR = "data/verified"
SAVE_EVERY = 200

ANIMAL_FATES = [
    "nothing_happened_to_animal",
    "not_real_animal",
    "animal_attacked_person",
    "animal_injured_or_killed",
    "animal_abandoned_or_neglected",
    "animal_threatened",
]
HARM_FATES = {
    "animal_injured_or_killed",
    "animal_abandoned_or_neglected",
    "animal_threatened",
}

# The model only answers factual questions; the yes/no decision is computed in code
STEP1_SCHEMA = {
    "type": "object",
    "properties": {
        "evidence": {"type": "string", "maxLength": 100},
        "animal_fate": {"type": "string", "enum": ANIMAL_FATES},
        "actor": {
            "type": "string",
            "enum": ["not_applicable", "defendant", "other_person", "unknown"],
        },
    },
    "required": ["evidence", "animal_fate", "actor"],
}

STEP2_SCHEMA = {
    "type": "object",
    "properties": {
        "harm_type": {
            "type": "string",
            "enum": [
                "intentional",
                "neglect_or_abandonment",
                "negligent_injury",
                "threat_only",
            ],
        },
        "animal_type": {
            "type": "string",
            "enum": ["pet", "stray", "livestock", "wildlife", "other"],
        },
        "court_finding": {
            "type": "string",
            "enum": ["convicted", "acquitted", "not_addressed", "unclear"],
        },
    },
    "required": ["harm_type", "animal_type", "court_finding"],
}

# Fixed instructions come first so vLLM prefix caching can reuse them across requests.
STEP1_PROMPT = """你是台灣刑事判決的分析助理。你會看到一份判決中所有提到動物的段落。

只關注「動物本身」的遭遇。人被打、人被威脅、人受傷，都與本題無關，即使事情的起因跟動物有關。

請依序填寫：
1. evidence：引用描述「動物本身遭遇」的原文（不超過 100 字）。如果動物本身沒有遭遇任何事，填空字串。
2. animal_fate：動物本身遭遇了什麼？
   - nothing_happened_to_animal：動物只是被提到，本身沒有受傷、被遺棄或被威脅
   - not_real_animal：不是真的動物（比喻、成語如「雞犬不寧」、人名綽號、地名店名、物品名）
   - animal_attacked_person：動物是加害者，咬傷、撞傷或攻擊人
   - animal_injured_or_killed：動物被打、被殺、被毒、被撞、受傷或死亡
   - animal_abandoned_or_neglected：動物被遺棄、棄養，或被疏於照顧而受害
   - animal_threatened：有人明確揚言要傷害或殺死動物
3. actor：造成動物受害或威脅動物的人是誰？
   - not_applicable：動物沒有受害也沒有被威脅
   - defendant：本案被告（包括共同被告、共犯）
   - other_person：告訴人、證人或其他人
   - unknown：看不出來是誰

案由：{title}

段落：
{text}"""

STEP2_PROMPT = """你是台灣刑事判決的分析助理。已知本案被告對動物有傷害、遺棄或威脅行為，請分類：

- harm_type：
  - intentional：故意傷害、殺害、虐待
  - neglect_or_abandonment：遺棄、棄養、疏於照顧
  - negligent_injury：過失造成動物受傷或死亡（例如車禍）
  - threat_only：只有揚言或威脅要傷害動物，沒有實際動手
- animal_type：pet（有主人的寵物）、stray（流浪動物）、livestock（家畜家禽）、wildlife（野生動物）、other
- court_finding：法院對「被告傷害動物這件事」的認定
  - convicted：認定有罪或認定事實成立
  - acquitted：判無罪或證據不足
  - not_addressed：判決沒有處理這部分
  - unclear：看不出來

案由：{title}

段落：
{text}"""


def call_vllm(
    model: str, prompt: str, max_tokens: int, schema: dict | None = None
) -> str:
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": 0,
        # Qwen3 family defaults to thinking mode, which emits long reasoning first
        "chat_template_kwargs": {"enable_thinking": False},
    }
    if schema:
        payload["response_format"] = {
            "type": "json_schema",
            "json_schema": {"name": "label", "schema": schema},
        }
    response = requests.post(
        f"{VLLM_BASE_URL}/chat/completions", json=payload, timeout=300
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"].strip()


def parse_json(text: str) -> dict | None:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


def ask(
    model: str, prompt: str, max_tokens: int, schema: dict
) -> tuple[dict | None, str]:
    try:
        answer = call_vllm(model, prompt, max_tokens, schema)
    except Exception as e:
        return None, f"ERROR: {e}"[:200]
    parsed = parse_json(answer)
    return parsed, "" if parsed else answer[:200]


def decide_involved(animal_fate: str | None, actor: str | None) -> str:
    if animal_fate not in HARM_FATES:
        return "no"
    if actor == "defendant":
        return "yes"
    if actor == "other_person":
        return "no"
    return "uncertain"


def label_case(model: str, row: dict) -> dict:
    snippet = row["passages"]
    result = {"jid": row["jid"]}
    if not snippet:
        return {**result, "step1_ok": False, "raw_response": "NO_TEXT"}

    title = str(row["title"])[:30]
    step1, raw = ask(
        model, STEP1_PROMPT.format(title=title, text=snippet), 200, STEP1_SCHEMA
    )
    if step1 is None:
        return {**result, "step1_ok": False, "raw_response": raw}
    step1["involved"] = decide_involved(step1.get("animal_fate"), step1.get("actor"))
    result.update({"step1_ok": True, **step1, "raw_response": ""})

    if step1.get("involved") in ("yes", "uncertain"):
        step2, raw = ask(
            model, STEP2_PROMPT.format(title=title, text=snippet), 80, STEP2_SCHEMA
        )
        result.update(step2 if step2 else {"step2_error": raw})

    return result


def main():
    flags = [a for a in sys.argv[1:] if a.startswith("--")]
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    model = args[0] if args else "Qwen/Qwen3-8B-MLX-4bit"
    limit = int(args[1]) if len(args) > 1 and args[1] != "all" else None
    concurrency = int(args[2]) if len(args) > 2 else 8
    suffix = "_test" if "--test" in flags else ""

    print(f"Model: {model} | concurrency: {concurrency}")
    print(f"Smoke test: {call_vllm(model, '只回答：好', max_tokens=5)}")

    df = pd.read_parquet(INPUT_FILE.format(suffix=suffix))
    if limit:
        df = df.head(limit)

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    model_tag = model.split("/")[-1]
    output_file = Path(OUTPUT_DIR) / f"stage3_{model_tag}_{PROMPT_VERSION}{suffix}.csv"
    labels_file = output_file.with_name(output_file.stem + "_labels.csv")

    done = pd.DataFrame()
    if labels_file.exists():
        done = pd.read_csv(labels_file)
        done = done[done["step1_ok"].eq(True)]
    todo = df[~df["jid"].isin(done.get("jid", []))]
    print(
        f"Cases: {len(df):,} | already labelled: {len(done):,} | to do: {len(todo):,}"
    )

    rows = todo[["jid", "title", "passages"]].to_dict("records")
    results = done.to_dict("records")
    monitor = ResourceMonitor(vllm_metrics_url=VLLM_METRICS_URL)
    monitor.start()
    start = time.time()

    with (
        ThreadPoolExecutor(max_workers=concurrency) as executor,
        tqdm(total=len(rows)) as pbar,
    ):
        futures = [executor.submit(label_case, model, r) for r in rows]
        for i, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            pbar.update(1)
            pbar.set_postfix_str(monitor.latest())
            if i % SAVE_EVERY == 0:
                pd.DataFrame(results).to_csv(labels_file, index=False)

    elapsed = time.time() - start
    monitor.stop()
    df_res = pd.DataFrame(results)
    df_res.to_csv(labels_file, index=False)
    df_out = df.drop(columns=["passages"]).merge(df_res, on="jid", how="left")
    df_out.to_csv(output_file, index=False)

    n = len(df_res)
    ok = df_res[df_res["step1_ok"].eq(True)]
    if rows:
        per_case = elapsed / len(rows)
        print(f"\nTime: {elapsed / 60:.1f} min | {per_case:.2f} sec/case")
        print(f"Estimate for 30,000 cases: {per_case * 30000 / 3600:.1f} hours")
    print(f"Step 1 parsed OK: {len(ok)}/{n}")
    if len(ok):
        print("\ninvolved:\n" + ok["involved"].value_counts().to_string())
        print("\nanimal_fate:\n" + ok["animal_fate"].value_counts().to_string())
        print("\nactor:\n" + ok["actor"].value_counts().to_string())
        if "harm_type" in ok:
            print("\nharm_type:\n" + ok["harm_type"].value_counts().to_string())

    monitor.print_summary()
    resource_file = output_file.with_name(output_file.stem + "_resources.csv")
    monitor.save(resource_file)
    print(f"\nSaved: {output_file}")
    print(f"Resource samples: {resource_file}")


if __name__ == "__main__":
    main()
