from __future__ import annotations

import json
import re

from app.llm.model import get_open_ai_model
from app.llm.schema import RouterIntent

ACTION_ID = "optimization.run_scan"

SYSTEM_PROMPT = """
You are an intent router for a FinOps chatbot.
Return only JSON with keys:
- intent: one of analyze, set_project, update_athena, rescan, help, chat
- action: string action id (always optimization.run_scan)
- profile_name: string|null
- project_name: string|null
- confidence: number 0..1
No extra keys, no markdown.
"""

_PROFILE_RE = re.compile(r"(?:with|using)?\s*profile(?:\s+name)?\s*(?:is|=|:)?\s*([A-Za-z0-9._-]+)", re.IGNORECASE)
_PROJECT_RE = re.compile(r"(?:project\s*(?:is|=|:)?\s*)([A-Za-z0-9._\-\s]{2,80})", re.IGNORECASE)


def _parse_command(prompt: str) -> RouterIntent | None:
    text = prompt.strip()
    if not text.startswith("/"):
        return None

    lower = text.lower()
    if lower.startswith("/rescan"):
        return RouterIntent(intent="rescan", confidence=1.0)
    if lower.startswith("/help"):
        return RouterIntent(intent="help", confidence=1.0)
    if lower.startswith("/athena") and "edit" in lower:
        return RouterIntent(intent="update_athena", confidence=1.0)
    if lower.startswith("/project"):
        name = text[len("/project") :].strip()
        return RouterIntent(intent="set_project", project_name=name or None, confidence=1.0)
    if lower.startswith("/analyze"):
        match = re.search(r"profile\s*=\s*([A-Za-z0-9._-]+)", text, re.IGNORECASE)
        profile_name = match.group(1) if match else None
        return RouterIntent(
            intent="analyze",
            action=ACTION_ID,
            profile_name=profile_name,
            confidence=1.0,
        )

    return RouterIntent(intent="chat", confidence=0.5)


def _heuristic_route(prompt: str) -> RouterIntent | None:
    text = prompt.strip()
    lower = text.lower()

    if not text:
        return RouterIntent(intent="chat", confidence=0.0)

    if "help" in lower:
        return RouterIntent(intent="help", confidence=0.9)

    if "rescan" in lower or "rerun" in lower:
        return RouterIntent(intent="rescan", confidence=0.9)

    if "athena" in lower and any(token in lower for token in {"edit", "update", "change"}):
        return RouterIntent(intent="update_athena", confidence=0.85)

    project_match = _PROJECT_RE.search(text)
    if lower.startswith("project ") and project_match:
        return RouterIntent(
            intent="set_project",
            project_name=project_match.group(1).strip(),
            confidence=0.8,
        )

    if any(token in lower for token in {"analyze", "analysis", "scan", "nat gateway", "idle nat"}):
        profile_match = _PROFILE_RE.search(text)
        profile_name = profile_match.group(1) if profile_match else None
        return RouterIntent(
            intent="analyze",
            action=ACTION_ID,
            profile_name=profile_name,
            confidence=0.78 if profile_name else 0.62,
        )

    return None


def _llm_route(prompt: str) -> RouterIntent | None:
    try:
        client, config = get_open_ai_model()
    except Exception:
        return None

    try:
        response = client.chat.completions.create(
            model=config.model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT.strip()},
                {"role": "user", "content": prompt},
            ],
        )
        content = (response.choices[0].message.content or "").strip()
        if not content:
            return None
        parsed = json.loads(content)
        parsed["action"] = ACTION_ID
        return RouterIntent.model_validate(parsed)
    except Exception:
        return None


def route(prompt: str) -> RouterIntent:
    parsed = _parse_command(prompt)
    if parsed is not None:
        return parsed

    heuristic = _heuristic_route(prompt)
    if heuristic is not None and heuristic.confidence >= 0.75:
        return heuristic

    llm = _llm_route(prompt)
    if llm is not None:
        return llm

    if heuristic is not None:
        return heuristic

    return RouterIntent(intent="chat", confidence=0.0)
