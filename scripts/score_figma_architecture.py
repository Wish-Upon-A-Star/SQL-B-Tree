import json
import sys
from pathlib import Path


CATEGORIES = {
    "narrative_clarity": 15,
    "architecture_fidelity": 20,
    "boundary_ownership_clarity": 15,
    "visual_hierarchy": 10,
    "layout_spacing_discipline": 10,
    "typography_readability": 10,
    "color_polish": 10,
    "information_density_control": 5,
    "presentation_readiness": 5,
}

REQUIRED_FLAGS = [
    "has_hero",
    "has_system_flow",
    "has_boundary_section",
    "has_score_snapshot",
    "has_notes_section",
]

PENALTY_KEYS = [
    "giant_wall_of_text",
    "chaotic_color_usage",
    "incorrect_flow",
    "ownership_conflict",
    "implementation_mismatch",
]


def fail(message: str) -> int:
    print(f"[fail] {message}", file=sys.stderr)
    return 1


def main() -> int:
    if len(sys.argv) != 2:
        return fail("usage: python scripts/score_figma_architecture.py <score.json>")

    path = Path(sys.argv[1])
    if not path.exists():
        return fail(f"file not found: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    scores = payload.get("scores", {})
    requirements = payload.get("requirements", {})
    penalties = payload.get("penalties", {})

    total = 0
    for key, maximum in CATEGORIES.items():
        value = scores.get(key)
        if not isinstance(value, (int, float)):
            return fail(f"missing numeric score: {key}")
        if value < 0 or value > maximum:
            return fail(f"score out of range for {key}: {value} / {maximum}")
        total += value

    penalty_total = 0
    for key in PENALTY_KEYS:
        value = penalties.get(key, 0)
        if not isinstance(value, (int, float)):
            return fail(f"invalid penalty: {key}")
        if value > 0:
            return fail(f"penalties must be zero or negative: {key}")
        penalty_total += value

    capped_total = total + penalty_total
    cap = 100

    if not all(requirements.get(flag, False) for flag in REQUIRED_FLAGS):
        cap = min(cap, 84)
    if scores["architecture_fidelity"] < 15:
        cap = min(cap, 89)
    if scores["boundary_ownership_clarity"] < 12:
        cap = min(cap, 89)
    if scores["visual_hierarchy"] < 8:
        cap = min(cap, 92)
    if scores["typography_readability"] < 8:
        cap = min(cap, 92)
    if scores["color_polish"] < 8:
        cap = min(cap, 94)

    final_total = min(capped_total, cap)

    qualifies_95 = (
        all(requirements.get(flag, False) for flag in REQUIRED_FLAGS)
        and scores["architecture_fidelity"] >= 18
        and scores["boundary_ownership_clarity"] >= 14
        and scores["visual_hierarchy"] >= 9
        and scores["layout_spacing_discipline"] >= 9
        and scores["typography_readability"] >= 9
        and scores["color_polish"] >= 9
        and penalty_total >= -2
        and final_total >= 95
    )

    print(f"raw_total={total:.2f}/100.00")
    print(f"penalty_total={penalty_total:.2f}")
    print(f"cap={cap:.2f}")
    print(f"final_total={final_total:.2f}/100.00")
    print(f"qualifies_95={'yes' if qualifies_95 else 'no'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
