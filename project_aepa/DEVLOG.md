# Development Log - Animal Abuse Case Analysis

This file has two parts:

- **Current State**: always up to date. How the pipeline works now, how to run it, what is next.
  Edit it whenever a script, command or plan changes.
- **Log**: dated entries, newest first. Why decisions were made. Never rewrite old entries;
  add a new one instead.

Research design: [animal-abuse-crime-linkage-data-plan.md](animal-abuse-crime-linkage-data-plan.md)

---

# Current State (updated 2026-11-26)

## Goal

Find criminal cases where the defendant harmed or threatened animals, link each defendant's
earlier and later judgments, and compare their criminal histories with a matched control
group. Research questions and limits are in the plan document.

## Setup

Installation, data download and where each kind of data lives: [README.md](README.md).
After setup, regenerate derived data with the commands below (steps 1-6). Do not run the
same step on two machines at once.

All scripts run from `project_aepa/`. Long-running scripts take a memory limit argument and
abort above it (macOS does not enforce RLIMIT_AS).

## Pipeline

Flow diagram: [PIPELINE.md](PIPELINE.md)

| # | Step | Script | Output | Status |
|---|---|---|---|---|
| 0 | Download and convert raw data | `download_monthly_packages.py`, `extract_rar.py`, `process_judgments.py` | `data/processed/` | ✅ |
| 1 | Split documents into sections | `build_structured.py` (uses `judgment_parser.py`) | `data/structured/` | 🔄 re-run with `--force` (attachment fix) |
| 2 | Defendant name hashes and cited prior case numbers | `build_identity_index.py` | `data/index/` | ✅ (see known issues) |
| 3 | Resolve cited case numbers to JIDs | `resolve_prior_refs.py`, `diagnose_prior_refs.py` | `data/index/prior_case_refs_resolved.parquet` | ✅ |
| 4 | Stage 1 v2: Animal Protection Act by cited articles or title | `build_stage1_v2.py` | `data/filtered/stage1_v2.csv` | ✅ (re-run after step 1) |
| 5 | Stage 2 v3: find candidates in facts sections (tags only) | `filter_stage2_v3.py` | `data/filtered/stage2_v3*.csv` | 🔜 test run |
| 6 | Build LLM input: Stage 1 ∪ Stage 2, passages from facts | `build_stage3_input.py` | `data/filtered/stage3_input.parquet` | 🔜 |
| 7 | Stage 3: LLM judges every candidate (resumable) | `filter_stage3_llm_verification_vllm.py`, `evaluate_stage3.py` | `data/verified/stage3_<model>.csv` | 🔜 |
| 8 | Exploratory analysis | `analyze_stage1_linkage.py`, `build_control_comparison.py` | `data/analysis/` | ⏸ paused until data checks pass |

Support scripts: `resource_monitor.py` (GPU/RAM sampling, memory guard),
`check_identity_linkage.py` (name / prior availability by year).
Kept for reference only: `filter_stage3_llm_verification{,_fast,_ultra}.py` (Ollama versions),
`filter_stage1_animal_protection_law.py`, `filter_stage2_crime_first_fulltext.py` (v2) and
`extract_candidate_fulltext.py` (replaced by steps 4-6 above).

### Commands

```bash
# 1. Structure all documents (~5 min, resumable; --force re-parses after parser changes)
python scripts/build_structured.py --test 50     # quick check on 50 files
python scripts/build_structured.py --force
python scripts/build_structured.py --report      # coverage report only

# 2-3. Linking indexes
python scripts/build_identity_index.py
python scripts/resolve_prior_refs.py [memory_limit_gb]

# 4-6. Stage 1, Stage 2, LLM input (add --test for a 300-batch sample + Stage 1 batches)
python scripts/build_stage1_v2.py
python scripts/filter_stage2_v3.py [--test 300]
python scripts/build_stage3_input.py [--test]

# 7. Stage 3 (needs a vLLM server on localhost:8000 serving Qwen3-8B MLX 4-bit).
#    Resumable: re-running skips documents already labelled.
python scripts/filter_stage3_llm_verification_vllm.py <model> [limit] [concurrency=8] [--test]
python scripts/evaluate_stage3.py                # against data/labels/gold_v0.csv

# 8. Exploratory analysis
python scripts/analyze_stage1_linkage.py [memory_limit_gb]
python scripts/build_control_comparison.py [k_controls] [memory_limit_gb]
```

### Structured data (`data/structured/`)

One Parquet per input batch. Sections are stored as character offsets into `full_text`
(`<section>_start` / `_end`, -1 if missing): header, main (主文), facts, reasons, combined
(事實及理由), statutes, attach, attach_facts (facts of an attached indictment). Also:
verdict flags from 主文, `laws`, `cites_animal_protection_act`, `defendant_hashes`.
Use `judgment_parser.section(text, row, "facts")` to slice.

Facts for analysis: `facts`, else `combined`, else `attach_facts`. Judgments with only
理由 (acquittals, appeals, Supreme Court) have no separate facts section by design.

## Next Steps

1. **Stage 3 on the test input (2,085 candidates, prompt p2)**, planned on the A6000
   machine, set up from the repo (README): `dvc pull`, `.env`, then `build_structured.py`,
   `build_stage1_v2.py`, `filter_stage2_v3.py --test 300`, `build_stage3_input.py --test`
   (should give the same 2,085 candidates), then
   `vllm serve Qwen/Qwen3-8B --max-model-len 8192 --port 8000` and
   `python scripts/filter_stage3_llm_verification_vllm.py Qwen/Qwen3-8B all 32 --test`.
   Labels from this model are a separate file (model name in the file name); compare
   with the MLX model on the same cases before mixing anything.
2. **Check 1 (case identification)**: ~150-200 labeled cases, stratified: Stage 1
   convictions (LLM recall), Stage 2 core, Stage 2 tagged, and animal word without a match
   (`stage2_v3_animal_no_match.csv`, Stage 2 misses). Gate for the full LLM run. The draft
   gold set `data/labels/gold_v0.csv` still needs human review.
3. Full Stage 2 v3 run, `build_stage3_input.py`, full Stage 3 run (~22k candidates;
   ~31 h on the Mac at concurrency 8, faster on the A6000; resumable). Scope rules (e.g.
   pigeon ransom out of scope) are applied in code from the LLM answers. Then
   `dvc add data/verified` and `dvc push`.
4. Unit of analysis = one defendant in one case: merge appeal levels, split co-defendants.
5. **Check 2 (linking)**: manually review ~50 links; rebuild the defendant index from
   `data/structured/` (fixes the co-defendant issue below).
6. Resume analysis: control comparison with procedure-type matching, name-based linking.

## Known Issues

- `build_identity_index.py` misses co-defendants listed on continuation lines (name on a
  line without 被告). The structured parser handles this.
- `match == normalized_word` links share a defendant name only 35% of the time: exclude.
- The prior-record window may capture co-defendants' or witnesses' records.
- About 2-4% of documents are not criminal (JID court code 4th character `V`, e.g. payment
  orders): exclude by code.
- Before 2000 only Supreme Court documents exist; defendant names are masked before ~2011.
- About two thirds of summary judgments whose facts are "引用附件" do not publish the
  attachment: no facts text. Stage 2 cannot find them; only Stage 1 (cited articles) can.

---

# Log

## 2026-11-26 - Stage 2 v3 and the Stage 2 / Stage 3 Split

### Stage 2 v3: facts sections, no crime-title filter
Searches only facts sections (facts, 事實及理由, attached indictment facts) of all
criminal judgments, for an animal word and a harm word within 30 chars in one sentence.
Stage 1 documents are scanned too, so Stage 1 convictions under the act measure recall.
First test (300 spread batches + Stage 1 batches): recall 95.8% (343/358); ~21k matches
outside Stage 1 projected over all batches.

### Misses: mostly unpublished attachments, plus a parser bug
All 15 missed Stage 1 convictions said their facts were in the attached indictment.
Sampling summary judgments: ~63% refer to an attachment for facts, and ~64% of those do
not publish it. A small share were parser misses: 【附件】 with brackets, or the indictment
appended without any 附件 heading. `judgment_parser.py` now accepts both (4 of the 15
recovered). Missing vocabulary: dog breeds (吉娃娃, 貴賓…) and 鬥 (鬥狗, 鬥雞) added.

### Tag, don't filter
A first fix removed noise by rules (quoted nicknames, product words, owner's dog biting,
pigeon ransom). Rejected: each rule can drop true cases (e.g. 黑貓 nearly removed a real
cat-drowning case; short quoted threats like 「要殺你的狗」), the rules were fitted to the
test sample, and each needs a justification. Decision: Stage 2 finds candidates and only
removes text that cannot be facts; likely noise is tagged. Stage 3 makes every judgment:
the LLM answers factual questions and scope is decided in code (pigeon ransom: animal
threatened by someone other than the defendant → no). Check 1 samples by tag to confirm
the LLM rejects tagged noise on its own. Cost: ~30k LLM candidates instead of ~16k.

### Test run after the changes (300 spread batches + Stage 1 batches)
- Stage 1 v2 after the parser fix: 524 documents, 362 convictions under the act (was 364:
  an indictment appended without a 附件 heading used to be read as the statutes appendix,
  so its charged articles counted as applied ones).
- Stage 2 recall on Stage 1 convictions: 97.5% (347/356); untagged matches only: 92.4%.
- Judgments whose facts exist only by reference to an unpublished attachment: 59,316 of
  321,421 with a facts section (18%). Largest coverage limit of keyword search.
- Matches outside Stage 1: 1,561 (core 809; tags: quoted/product 284, owner/traffic 233,
  pigeon ransom 237). Projected over all batches: ~21k (core ~11k).
- Animal word in facts but no match: 2,581 (Check 1 stratum for Stage 2 misses).
- Stage 3 test input: 2,085 candidates; passages from facts 95%, mean 842 chars.

### First 50 LLM labels on the new input (prompt p1)
5.1 sec/case at concurrency 8 (~31 h for ~22k candidates), 50/50 parsed. Stage 1
documents 10/10 yes; tagged noise 15/16 no (nicknames, pigeon ransom, dog bites). Errors:
- `animal_harmed_person` was read as "animal harmed by a person": two shootings of birds
  by defendants got this label and were missed. Renamed `animal_attacked_person`, with
  the animal named as the attacker.
- Idiom 雞犬不寧 in a threat was labelled a threatened animal: idioms added to
  `not_real_animal`.
- Co-defendants were labelled `other_person`: `defendant` now includes co-defendants.
Outputs now carry a prompt version (`PROMPT_VERSION`, now p2) so labels from different
prompts never mix in one resumable file.

### Working across machines
Only code and `data/processed.dvc` are in git. Human labels (`data/labels/`) cannot be
regenerated, so they are now tracked in git. `build_stage1_v2.py` no longer reads the v1
output file (timestamped name, would differ on another machine); it applies the same
title rule itself (verified: identical 441 documents). Setup and the data location table
live in README; DEVLOG only points there. Added `CLAUDE.md` with working rules.

### Resource monitor on NVIDIA
`resource_monitor.py` detects the GPU backend once: nvidia-smi if present, else ioreg on
macOS, else none. Memory pressure is read on macOS only.

### Stage 3 input and resume
`build_stage3_input.py` replaces `extract_candidate_fulltext.py`: candidates = Stage 1 ∪
Stage 2, passages around animal mentions taken from facts sections (fallback: reasons,
then full text). Stage 1 documents go through the LLM for harm type and as known
positives for LLM recall. The vLLM script now reads this file and skips documents
already labelled, so a multi-day run can be restarted.

## 2026-11-24 - Control Comparison, Source Check, Structuring

### First control comparison: hold conclusions

`build_control_comparison.py`: 343 first-instance Stage 1 judgments vs 3,430 controls
matched on court and year. Animal Protection Act defendants had *fewer* cited priors (6.7% vs
15.0%, RR 0.45) and fewer later offences (2.3% vs 8.3%). Not interpretable yet:

- Stage 1 mixes abuse with breeding, selling and abandonment offences.
- Controls are general criminal defendants, dominated by repeat drug and DUI offenders.
- Priors are only seen when the judgment cites a case number (mostly for recidivism), and
  minor summary judgments may cite fewer; procedure type was not matched.
- Violent outcomes had 1 event per group: no power.

Decision: stop analysis and first confirm the data is what we need.

### Data source reliability

Decision: the Judicial Yuan open data is the only source; no extra collection. Documented
limits in plan section 6.2. Checked on our data: 0.02% documents lack text; before 2000 only
the Supreme Court is covered; 2021 dip (COVID); 2026 is partial.

### Check strategy

Stages 1-3 answer one question ("did the defendant harm an animal in this case?"), so they
are checked together with one labeled sample instead of stage by stage. Linking is checked
separately.

### Structuring before checking

Judgments follow a fixed template (主文 / 事實 / 理由 / 附錄法條 / 附件). Parsing it lets
Stage 2 search only the facts, the LLM read focused text, and Stage 1 use cited statutes
instead of titles. So structuring comes before Check 1.

Results on all 7.74M documents (4-5 min): 主文 found in 99.8%+ of judgments, defendants in
97%, full names in 96-98% from 2011. Fixes found while testing: sentencing records
(宣示判決筆錄, numbered headings), Supreme Court defendants labelled only 上訴人, address
fragments taken as names.

### Stage 1 by statutes, not titles

Titles hide secondary charges ("毀損等"). First attempt flagged any mention of 動物保護法
and caught 529 dog-bite negligence cases (they cite 第7條, owner's duty) and 野生動物保護法
(old name of the Wildlife Act). Final rule: a criminal article of the act (25, 25-1, 26,
27-1, and 30 before the 2015 amendment) in the statutes appendix, 主文 or reasons.

Compared with the 441 title matches (before the `--force` re-run): 486 flagged, 384 in both,
102 only by statutes (titles such as 毀損, 槍砲, 竊盜, 傷害, 公共危險; 73 guilty — exactly the
co-occurring crimes this study needs), 57 only by title (mostly rulings, Wildlife Act, appeals
dismissed; 3 real misses).

## 2026-11-23 - Cross-Judgment Linking

Defendant full names are public from about 2011, so cross-judgment linking is possible after
all (plan section 2.2.1). Two signals:

- Case numbers cited in prior-record text (前因…案件), resolved to JIDs: 3.05M references,
  71.1% resolved when a court or 本院 is named (82.2% excluding unpublished 毒聲 rulings).
  Summary courts have their own codes (士簡 → SLEM), handled by `court_fallback`.
- HMAC name hashes (`NAME_HASH_KEY`), never plain names.

Validation: 87.3% of checkable links share a defendant name hash (exact 87.4%,
court_fallback 88.2%, normalized_word 35.1% → exclude). Stage 1: 29 judgments link back to
49 priors, 14 are cited by 23 later documents (first look only, no control).

## 2026-11-22 - Real Bottleneck, vLLM, Label Design, Stage 2 v2

### The 20 sec/case bottleneck was file I/O, not the LLM

A single Ollama call took ~0.07 s. Each case took ~20 s because `get_case_fulltext()`
rescanned up to 13,042 Parquet files per case. Fix: `extract_candidate_fulltext.py`
scans once (6 min) and writes all candidate texts to one 154 MB Parquet file.

Lesson: profile before optimizing. Prompt shortening, Ollama "parallel" threads and the
framework switch were all aimed at the wrong bottleneck.

### Ollama vs vLLM (vllm-metal)

- Ollama ThreadPool "parallelism" gave 1.11x: requests are effectively serialized.
- vllm-metal with Qwen3-8B (MLX 4-bit), concurrency 8: ~2.7 s/case for a yes/no prompt,
  ~7.3 s/case for the current two-step structured prompt.
- Concurrency 16 was no faster: GPU already at ~99% utilization, requests queued in vLLM.
  Keep concurrency at 8. Min free RAM ~2 GB, memory pressure stayed normal.
- Qwen3 defaults to thinking mode; must pass `enable_thinking: false`.

### Label design iterations (first 50 candidates, draft gold set)

| Version | Design | Precision | Recall |
|---|---|---|---|
| Single yes/no, qwen2.5:3b | free text | "68% yes" (no gold set, not accuracy) | - |
| Many exclusion examples | few-shot | 4% yes, rejected real 動保法 cases | - |
| 7 fields in one pass | JSON schema | several fields contradictory | - |
| Step 1: model decides involved | JSON schema | 0.20 | 1.00 |
| Step 1: model answers facts, code decides | JSON schema | 0.83 | 1.00 |

Findings:
- An 8B model is reliable on factual questions ("what happened to the animal", "who did it")
  but poor at combining them into a judgment. Compute the final decision in code.
- Under constrained JSON decoding the model defaults to the first enum option when unsure.
- Synthetic few-shot examples were rejected: not representative of real judgment language,
  and changing two things at once hides which change helped. If examples are needed, use
  real judgments outside the evaluation set.
- Only 5 true positives in the 50-case gold set; results are not conclusive.

### Research scope re-check

Re-reading the plan showed the current LLM step covers only signals A/B (abuse or threat in
the current case). The retrospective analysis mainly needs signal C (prior animal-related
records), prior-record sequences, and a control group. Labeling decisions are in section
7.3.1 of the plan.

### Stage 2 v2

A 500-batch sample showed drug and theft cases are almost pure noise for animal harm
(nicknames like 阿狗, stolen pet food), and firearms cases match a standard ballistics
phrase (穿入豬隻皮肉層). Decision: exclude drugs and theft from Stage 2 (handled by the
signal C regex instead), keep firearms with a 30-char proximity rule, strip boilerplate,
remove exclusion keywords (recall first; the LLM handles them).

---

## 2026-11-21 - Stage 3 LLM 驗證效能優化

### 問題背景

完成 Stage 1 和 Stage 2 後，得到 17,267 件候選案件需要用 LLM 驗證是否真的是動物虐待案件。

**初始狀況：**
- 總候選案件：17,267 件
  - Stage 1 (動物保護法)：442 件
  - Stage 2 (Crime-first fulltext)：16,825 件
- 測試模型：qwen2.5:3b (本地 Ollama，M3 Pro GPU)
- 初始速度：**21.8 秒/件**
- 預估總時間：**104 小時 (4.3 天)**

### 問題分析

**為什麼這麼慢？**

1. **判決書太長**
   - 原始設計：每件送 3000 字給 LLM
   - 中文 token 密度高
   - M3 Pro GPU 雖然有用，但比專業 GPU 慢

2. **Prompt 太複雜**
   - 要求 LLM 生成複雜的 JSON 格式
   - 包含多個欄位：is_abuse, confidence, abuse_type, false_positive_reason
   - JSON 生成增加處理時間

3. **串行處理**
   - 一次只處理一件案件
   - 沒有利用並行能力

### 嘗試的解決方案

#### 優化 1：減少文本長度 + 簡化 Prompt

**實作：** `filter_stage3_llm_verification_fast.py`

**改進：**
- 文本：3000 字 → 800 字（只取動物關鍵字附近段落）
- Prompt：複雜 JSON → 簡單「是/否」問題
- 參數：`num_predict: 10`（只需 1-2 個 token）

**結果：**
- 速度：~21 秒/件（幾乎沒改善 😞）
- 瓶頸不在文本長度，而在 LLM 本身的推理速度

#### 優化 2：結構化輸出 + Few-shot + 並行處理 ⭐

**實作：** `filter_stage3_llm_verification_ultra.py`

**關鍵改進：**
1. **並行處理**（最重要！）
   - 用 `ThreadPoolExecutor` 同時處理 4 件案例
   - 預期：速度提升 3-4 倍

2. **結構化輸出**
   - 用 Ollama 的 `format` 參數強制 JSON Schema
   - 減少解析錯誤

3. **Few-shot Examples**
   - 在 prompt 中加入 3 個範例
   - 提高準確度

4. **更短文本**
   - 500 字（取第一個動物關鍵字附近）

**預期結果：**
- 速度：5-8 秒/件
- 總時間：**4-8 小時**（可接受！）

### Research 參考

#### 結構化輸出最佳實踐
- [Ollama Structured Outputs](https://ollama.com/blog/structured-outputs)
- 要點：用 JSON Schema + temperature=0

#### 兩階段方法（Summarize → Classify）
- [Legal Doc Summarization](https://github.com/aryanmangal769/Legal-Doc-Summarization-LLM-LoRA-RAG)
- 研究顯示：增加 30-40% 時間，但可提高準確度
- 決定：先不用（已經夠慢了），但可作為未來改進方向

### 決策考量

**為什麼選擇並行處理？**
- ✅ 最直接的加速方式（3-4x）
- ✅ 不犧牲準確度
- ✅ 充分利用 M3 Pro 多核心
- ⚠️ 注意：Ollama 可能有並行限制，需要測試

**為什麼不用 Cloud API？**
- 💰 成本：~$50-100 USD
- 🔒 資料隱私：判決書包含敏感資訊
- ⏱️ 本地優化後時間可接受（4-8 小時）

**為什麼不用兩階段方法？**
- ⏱️ 增加 30-40% 時間（不划算）
- 📊 誤差傳播風險
- 🎯 目前方法已經夠快

### 下一步

1. **立即測試 ULTRA 版本**
   ```bash
   python scripts/filter_stage3_llm_verification_ultra.py qwen2.5:3b 50
   ```

2. **如果速度 <8 秒/件**
   - 跑完整版 17,267 件
   - 預估 4-8 小時（可過夜執行）

3. **如果速度還是 >15 秒/件**
   - 考慮只驗證高風險案由（過失傷害、公共危險）
   - 其他案由人工抽查

### 學到的教訓

1. **GPU 加速不是萬能**
   - M3 Pro 雖然有 GPU，但比專業 GPU 慢很多
   - 本地 LLM 的推理速度有限制

2. **並行處理是關鍵**
   - 單純優化 prompt 效果有限
   - 並行處理可以直接 3-4x 加速

3. **需要平衡準確度和速度**
   - 過度優化可能影響準確度
   - 68% true positive rate 是可接受的起點

---

## 未來可能的改進方向

### 短期
- [ ] 測試不同 worker 數量（2, 4, 8）
- [ ] 測試 qwen2.5:7b 是否更準確
- [ ] 加入錯誤重試機制

### 中期
- [ ] 兩階段方法實驗（Summarize → Classify）
- [ ] Fine-tune 本地模型（專門針對動物虐待判決）
- [ ] 使用更小的模型（qwen2.5:1.5b）測試速度

### 長期
- [ ] 建立 Ground Truth 資料集
- [ ] 評估不同模型的準確度（Qwen vs Llama vs Gemma）
- [ ] 探索量化技術（4-bit vs 8-bit）

---

## 效能記錄

| 版本 | 文本長度 | Prompt 類型 | 並行 | 速度 (秒/件) | 總時間估計 |
|------|---------|------------|------|-------------|-----------|
| v1 (原始) | 3000字 | 複雜 JSON | ❌ | 21.8 | 104小時 |
| v2 (fast) | 800字 | 簡單是/否 | ❌ | ~21 | ~100小時 |
| v3 (ultra) | 500字 | Few-shot JSON | ✅ 4 workers | ? | 4-8小時? |

---

## 參考資料

- [Ollama Structured Outputs](https://ollama.com/blog/structured-outputs)
- [Legal Lay Summarization](https://link.springer.com/article/10.1007/s10462-025-11392-7)
- [Best Local LLMs for Structured Output](https://insiderllm.com/guides/structured-output-local-llms/)
