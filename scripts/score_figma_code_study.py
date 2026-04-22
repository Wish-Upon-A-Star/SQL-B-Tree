import json
import sys
from pathlib import Path

MAX_SCORES = {
    "study_startability": 15,
    "code_traceability": 20,
    "request_lifecycle_explanation": 15,
    "data_structure_understanding": 15,
    "role_boundary_clarity": 10,
    "snippet_quality": 10,
    "study_sequence_design": 10,
    "layout_readability": 5,
}

REQUIRED_FLAGS = [
    "has_four_real_file_paths",
    "has_eight_real_symbols",
    "has_four_code_snippets",
    "has_four_step_study_order",
    "has_file_level_role_boundary",
    "has_request_lifecycle_map",
]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: python scripts/score_figma_code_study.py <score.json>")
        return 1

    path = Path(sys.argv[1])
    data = json.loads(path.read_text(encoding="utf-8"))
    cats = data["categories"]
    penalties = data.get("penalties", {})
    flags = data["required_flags"]

    raw_total = 0.0
    for key, max_value in MAX_SCORES.items():
        value = float(cats.get(key, 0))
        if value < 0 or value > max_value:
            raise ValueError(f"{key} must be between 0 and {max_value}")
        raw_total += value

    penalty_total = 0.0
    for value in penalties.values():
        penalty_total += float(value)

    cap = 100.0
    if not flags.get("has_four_real_file_paths", False):
        cap = min(cap, 70.0)
    if not flags.get("has_eight_real_symbols", False):
        cap = min(cap, 72.0)
    if not flags.get("has_four_code_snippets", False):
        cap = min(cap, 75.0)
    if not flags.get("has_four_step_study_order", False):
        cap = min(cap, 78.0)
    if not flags.get("has_request_lifecycle_map", False):
        cap = min(cap, 80.0)
    if not flags.get("has_file_level_role_boundary", False):
        cap = min(cap, 84.0)

    if cats["code_traceability"] < 17:
        cap = min(cap, 86.0)
    if cats["data_structure_understanding"] < 12:
        cap = min(cap, 88.0)

    final_total = min(raw_total + penalty_total, cap)

    qualifies_95 = (
        all(flags.get(flag, False) for flag in REQUIRED_FLAGS)
        and cats["study_startability"] >= 14
        and cats["code_traceability"] >= 19
        and cats["request_lifecycle_explanation"] >= 14
        and cats["data_structure_understanding"] >= 14
        and cats["role_boundary_clarity"] >= 9
        and cats["snippet_quality"] >= 9
        and cats["study_sequence_design"] >= 9
        and cats["layout_readability"] >= 4
        and penalty_total >= -2
        and final_total >= 95
    )

    print(f"raw_total={raw_total:.2f}/100.00")
    print(f"penalty_total={penalty_total:.2f}")
    print(f"cap={cap:.2f}")
    print(f"final_total={final_total:.2f}/100.00")
    print(f"qualifies_95={'yes' if qualifies_95 else 'no'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
