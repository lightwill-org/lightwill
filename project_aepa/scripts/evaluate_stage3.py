"""
Compare Stage 3 step-1 labels against a manually labeled gold set.

Only gold rows with involved in {yes, no} are scored; uncertain/unverified are
listed separately for manual review.

Usage:
    python scripts/evaluate_stage3.py <labeled_result.csv> [gold.csv]
"""

import sys

import pandas as pd


def main():
    result_file = sys.argv[1]
    gold_file = sys.argv[2] if len(sys.argv) > 2 else "data/labels/gold_v0.csv"

    gold = pd.read_csv(gold_file)
    pred = pd.read_csv(result_file)[["jid", "involved", "actor", "evidence"]]
    df = gold.merge(pred, on="jid", how="inner", suffixes=("_gold", "_pred"))
    print(f"Matched {len(df)} of {len(gold)} gold rows")

    scored = df[df["involved_gold"].isin(["yes", "no"])].copy()
    # Treat model "uncertain" as a positive: it gets flagged for human review
    scored["pred_pos"] = scored["involved_pred"].isin(["yes", "uncertain"])
    scored["gold_pos"] = scored["involved_gold"] == "yes"

    tp = (scored.pred_pos & scored.gold_pos).sum()
    fp = (scored.pred_pos & ~scored.gold_pos).sum()
    fn = (~scored.pred_pos & scored.gold_pos).sum()
    tn = (~scored.pred_pos & ~scored.gold_pos).sum()

    print(f"\nScored: {len(scored)} (TP={tp} FP={fp} FN={fn} TN={tn})")
    print(f"Precision: {tp / (tp + fp):.2f}" if tp + fp else "Precision: n/a")
    print(f"Recall:    {tp / (tp + fn):.2f}" if tp + fn else "Recall: n/a")

    cols = [
        "jid",
        "title",
        "involved_gold",
        "involved_pred",
        "actor",
        "note",
        "evidence",
    ]
    pd.set_option("display.max_colwidth", 50)
    pd.set_option("display.width", 250)

    errors = scored[scored.pred_pos != scored.gold_pos]
    if len(errors):
        print("\nMismatches:")
        print(errors[cols].to_string(index=False))

    pending = df[~df["involved_gold"].isin(["yes", "no"])]
    if len(pending):
        print("\nGold uncertain/unverified (not scored):")
        print(pending[cols].to_string(index=False))


if __name__ == "__main__":
    main()
