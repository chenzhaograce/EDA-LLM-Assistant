from __future__ import annotations

import os
from typing import Any

from eda_llm_assistant.config import LLMConfig


def llm_summarize(cfg: LLMConfig, eda_payload: dict[str, Any]) -> str | None:
    if not cfg.enabled:
        return None

    api_key = os.getenv(cfg.api_key_env)
    if not api_key:
        return (
            f"LLM summary enabled, but env var `{cfg.api_key_env}` is not set. "
            "Skipping LLM summary."
        )

    if cfg.provider != "openai":
        return f"Unsupported LLM provider: {cfg.provider}"

    try:
        from openai import OpenAI
    except Exception as e:
        return f"OpenAI client import failed: {e}"

    client = OpenAI(api_key=api_key)

    # Keep prompt compact; we pass structured EDA results only (no raw data)
    messages = [
        {"role": "system", "content": cfg.system_prompt},
        {
            "role": "user",
            "content": (
                f"{cfg.user_prompt}\n\n"
                "EDA JSON:\n"
                f"{eda_payload}"
            ),
        },
    ]

    try:
        resp = client.chat.completions.create(
            model=cfg.model,
            messages=messages,
            temperature=0.2,
        )
        return resp.choices[0].message.content or ""
    except Exception as e:
        return f"LLM call failed: {e}"

