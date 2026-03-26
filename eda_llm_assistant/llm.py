from __future__ import annotations

import os
from typing import Any

from eda_llm_assistant.config import LLMConfig


def _normalize_gemini_model_id(model: str) -> str:
    """Strip accidental prefixes/suffixes from AI Studio copy-paste."""
    m = (model or "").strip().strip('"').strip("'")
    if m.startswith("models/"):
        m = m[len("models/"):]
    return m


def llm_summarize(cfg: LLMConfig, eda_payload: dict[str, Any]) -> str | None:
    if not cfg.enabled:
        return None

    api_key = os.getenv(cfg.api_key_env)
    if not api_key:
        return (
            f"LLM summary enabled, but env var `{cfg.api_key_env}` is not set. "
            "Skipping LLM summary."
        )

    if cfg.provider not in ("openai", "gemini"):
        return f"Unsupported LLM provider: {cfg.provider}"

    try:
        if cfg.provider == "openai":
            from openai import OpenAI

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

            resp = client.chat.completions.create(
                model=cfg.model,
                messages=messages,
                temperature=0.2,
            )
            return resp.choices[0].message.content or ""

        # Gemini (Google AI Studio / Gemini API)
        # Uses the Generative Language API endpoint:
        # https://ai.google.dev/gemini-api/docs
        from urllib.error import HTTPError
        from urllib.parse import urlencode
        from urllib.request import Request, urlopen

        import json

        # Gemini's `generateContent` takes a single prompt in `contents[].parts[].text`.
        # We fold system_prompt into the user prompt to keep the payload simple.
        prompt = (
            f"{cfg.system_prompt}\n\n"
            f"{cfg.user_prompt}\n\n"
            "EDA JSON:\n"
            f"{eda_payload}"
        )

        model_id = _normalize_gemini_model_id(cfg.model)
        if not model_id:
            return "Gemini call failed: empty model id (set `llm.model` to a model id from AI Studio, e.g. `gemini-2.5-pro`)."

        endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model_id}:generateContent"
        url = f"{endpoint}?{urlencode({'key': api_key})}"

        payload = {
            "contents": [
                {"role": "user", "parts": [{"text": prompt}]},
            ],
            "generationConfig": {"temperature": 0.2},
        }

        req = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(req, timeout=90) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            try:
                err_body = e.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = ""
            hint = (
                " If this is HTTP 404, your `llm.model` is wrong or no longer available for this API key—"
                "open AI Studio → Get code / model dropdown and copy the **exact model id**, "
                "or list models: "
                "`curl 'https://generativelanguage.googleapis.com/v1beta/models?key=YOUR_KEY'`."
            )
            return f"Gemini call failed: {e}. Response: {err_body or '(empty)'}{hint}"
        except Exception as e:
            return f"Gemini call failed: {e}"

        # Expected shape:
        # { "candidates": [ { "content": { "parts": [ { "text": "..." } ] } } ] }
        candidates = data.get("candidates") or []
        if not candidates:
            return f"Gemini call failed: {data}"

        parts = (candidates[0].get("content") or {}).get("parts") or []
        texts = [p.get("text") for p in parts if isinstance(p, dict) and p.get("text")]
        return ("".join(texts) or "").strip() or f"Gemini call returned no text: {data}"
    except Exception as e:
        return f"LLM call failed: {e}"

