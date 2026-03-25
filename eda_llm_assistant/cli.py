from __future__ import annotations

import argparse
import json
from pathlib import Path

from eda_llm_assistant.config import load_config
from eda_llm_assistant.pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="EDA LLM Assistant: configurable EDA + report generator")
    p.add_argument("--config", "-c", required=True, help="Path to config.yaml")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_config(args.config)
    result = run_pipeline(cfg)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

