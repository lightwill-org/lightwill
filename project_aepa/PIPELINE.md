# Pipeline Flow

From raw open data to analysis. Renders on GitHub and in VS Code's Markdown preview.
Status and commands: see *Current State* in [DEVLOG.md](DEVLOG.md). Update this diagram
whenever a step's status or design changes.

Legend: ✅ done · 🔄 needs re-run · 🔜 next · ⏸ paused · 🧪 manual check (gate)

Division of work in case identification:
- **Stage 2 finds candidates, it does not judge them.** Deterministic and recall-oriented.
  It removes only text that cannot be facts (non-facts sections, fixed boilerplate,
  legal-norm sentences). Likely noise (nicknames, an owner's dog biting someone,
  pigeon ransom) is tagged, not dropped.
- **Stage 3 makes every judgment.** The LLM answers factual questions; whether a case
  counts (scope) is decided in code from those answers, so scope changes need no re-run.
- **Check 1 tests both**, with a sample stratified by source and tag.

```mermaid
flowchart TD
    RAW["Judicial Yuan open data<br/>367 monthly packages (RAR, 52 GB)"]
    RAW -->|"download_monthly_packages → extract_rar → process_judgments"| P0

    P0["<b>[0] data/processed</b> ✅<br/>summary CSV: 7.73M documents<br/>13,042 Parquet batches with full text"]

    P0 --> S1["<b>[1] Structuring</b> ✅<br/>build_structured + judgment_parser<br/>→ data/structured<br/>section offsets · verdict flags · laws<br/>Animal Protection Act flag · defendant hashes"]
    P0 --> S2["<b>[2] Defendant & prior index</b> ✅<br/>build_identity_index<br/>→ name hashes, cited prior case numbers"]
    S2 --> S3["<b>[3] Resolve priors to JIDs</b> ✅<br/>resolve_prior_refs<br/>71% resolved · 87% share defendant name"]

    subgraph ID ["Case identification: did the defendant harm an animal?"]
        ST1["<b>[4] Stage 1 v2</b> ✅<br/>build_stage1_v2<br/>cited criminal articles OR title, minus Wildlife Act<br/>526 documents · 364 convictions under the act"]
        ST2["<b>[5] Stage 2 v3: find candidates</b> 🔜<br/>filter_stage2_v3<br/>facts sections · all titles<br/>animal + harm word in one sentence<br/>tags only, nothing dropped"]
        ST6["<b>[6] Build LLM input</b> 🔜<br/>build_stage3_input<br/>Stage 1 ∪ Stage 2<br/>passages around animal mentions in facts"]
        ST7["<b>[7] Stage 3 LLM: judge</b> 🔜<br/>vLLM + Qwen3-8B · resumable<br/>step 1: factual questions · decision in code<br/>step 2: harm type"]
        C1{{"🧪 Check 1 · gate for full LLM run<br/>stratified sample:<br/>Stage 1 convictions (LLM recall)<br/>Stage 2 core · Stage 2 tagged<br/>animal word, no match (Stage 2 misses)"}}
        ST1 -.->|"known positives:<br/>Stage 2 recall, vocabulary"| ST2
        ST1 --> ST6
        ST2 --> ST6 --> ST7
        ST7 --- C1
    end

    S1 --> ST1
    S1 --> ST2

    U["<b>[8] Unit of analysis</b> 🔜<br/>one defendant in one case<br/>merge appeal levels · split co-defendants"]
    C2{{"🧪 Check 2<br/>review 50 links manually"}}
    ST7 --> U
    S3 -->|"link earlier and later judgments"| U
    U --- C2

    A["<b>[9] Analysis</b> ⏸<br/>backward: priors before the case<br/>forward: offences after the case<br/>matched controls: court · year · procedure type"]
    U --> A --> R["Results and report"]

    classDef done fill:#d9f2e3,stroke:#2e8b57,color:#111
    classDef next fill:#fff4cc,stroke:#c9a400,color:#111
    classDef paused fill:#eeeeee,stroke:#888,color:#111
    classDef gate fill:#ffe0e0,stroke:#c0392b,color:#111
    class P0,S1,S2,S3 done
    class ST1 done
    class ST2,ST6,ST7,U next
    class A paused
    class C1,C2 gate
```

## Outputs by step

| Step | Output |
|---|---|
| 0 | `data/processed/*_summary.csv`, `data/processed/*_batch*.parquet` |
| 1 | `data/structured/*.parquet` |
| 2 | `data/index/defendants.parquet`, `name_freq.parquet`, `prior_case_refs.parquet`, `court_names.parquet` |
| 3 | `data/index/prior_case_refs_resolved.parquet` |
| 4 | `data/filtered/stage1_v2.csv` |
| 5 | `data/filtered/stage2_v3.csv`, `stage2_v3_stage1_missed.csv`, `stage2_v3_animal_no_match.csv` |
| 6 | `data/filtered/stage3_input.parquet` |
| 7 | `data/verified/stage3_<model>.csv` (+ `_labels.csv`, resumable), gold set `data/labels/gold_v0.csv` |

Test runs write the same names with a `_test` suffix.

## Known coverage limit

About two thirds of summary judgments that say their facts are in the attached indictment
do not publish the attachment. Stage 2 cannot see those facts; such cases are found only
when they cite the Animal Protection Act (Stage 1). Stage 2 reports the count
(`facts_by_reference_only`).
| 9 | `data/analysis/` (exploratory so far) |
