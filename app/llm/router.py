from __future__ import annotations

import json
import re
from typing import Literal

from app.llm.model import get_open_ai_model
from app.llm.schema import RouterIntent

ACTION_ID = "optimization.run_scan"

SYSTEM_PROMPT = """
You are an intent router for a FinOps chatbot.
Return only JSON with keys:
- intent: one of analyze, set_project, update_athena, rescan, retry, help, chat
- action: string action id (always optimization.run_scan)
- profile_name: string|null
- project_name: string|null
- target_service: string|null (for example nat, ec2, rds, s3)
- account_scope: one of current, all, account
- target_account_id: string|null (12-digit AWS account id when account_scope=account)
- confidence: number 0..1
No extra keys, no markdown.
"""

_PROFILE_RE = re.compile(r"(?:with|using)?\s*profile(?:\s+name)?\s*(?:is|=|:)?\s*([A-Za-z0-9._\-\]\[(),:;]+)", re.IGNORECASE)
_PROJECT_RE = re.compile(r"(?:project\s*(?:is|=|:)?\s*)([A-Za-z0-9._\-\s]{2,80})", re.IGNORECASE)
_SCOPE_RE = re.compile(r"scope\s*=\s*(current|all|account(?::[A-Za-z0-9._-]+)?)", re.IGNORECASE)
_ACCOUNT_ID_RE = re.compile(r"\b([0-9]{12})\b")
_STANDALONE_ACCOUNT_ID_RE = re.compile(r"(?<![A-Za-z0-9_-])([0-9]{12})(?![A-Za-z0-9_-])")
_PROFILE_TOKEN_RE = re.compile(r"\b[A-Za-z][A-Za-z0-9._-]*-[A-Za-z0-9._-]+\b")
_RETRY_PHRASES = {"try again", "retry", "run again", "again"}
_SERVICE_KEYWORDS = {
    "nat": {"nat", "nat gateway", "natgateway", "idle nat"},
    "ec2": {"ec2", "elastic compute cloud"},
    "rds": {"rds", "relational database service"},
    "s3": {"s3", "simple storage service"},
}


def _extract_target_service(text: str) -> str | None:
    lower = text.lower()
    for service, keywords in _SERVICE_KEYWORDS.items():
        if any(keyword in lower for keyword in keywords):
            return service
    return None


def _sanitize_profile_candidate(raw: str | None) -> str | None:
    value = (raw or "").strip()
    if not value:
        return None
    value = value.strip("`'\"")
    value = re.sub(r"[\]\[(){}.,;:!?]+$", "", value)
    return value or None


def _contains_analysis_intent(text: str) -> bool:
    lower = text.lower()
    return any(token in lower for token in {"analyze", "analyse", "analysis", "scan", "nat gateway", "idle nat"})


def _infer_profile_from_text(text: str) -> str | None:
    explicit = _PROFILE_RE.search(text)
    if explicit:
        return _sanitize_profile_candidate(explicit.group(1))

    if not _contains_analysis_intent(text):
        return None

    candidates = {_sanitize_profile_candidate(match.group(0)) for match in _PROFILE_TOKEN_RE.finditer(text)}
    cleaned = [item for item in candidates if item and not _ACCOUNT_ID_RE.fullmatch(item)]
    if len(cleaned) == 1:
        return cleaned[0]
    return None


def _is_retry_phrase(text: str) -> bool:
    lower = text.strip().lower()
    if lower in _RETRY_PHRASES:
        return True
    return "try again" in lower or "run again" in lower or "retry" in lower


def _parse_scope_token(token: str) -> tuple[Literal["current", "all", "account"], str | None]:
    clean = (token or "").strip().lower()
    if clean.startswith("account:"):
        account_ref = clean.split(":", 1)[1].strip()
        return "account", (account_ref or None)
    if clean in {"account", "all", "current"}:
        return clean, None
    return "current", None


def _extract_scope_and_account(text: str) -> tuple[Literal["current", "all", "account"], str | None]:
    scope_match = _SCOPE_RE.search(text)
    if scope_match:
        return _parse_scope_token(scope_match.group(1))

    lower = text.lower()
    if "all accounts" in lower or "across all accounts" in lower:
        return "all", None

    explicit_account = re.search(r"account\s*[:=]\s*([0-9]{12})", lower)
    if explicit_account:
        return "account", explicit_account.group(1)

    if "specific account" in lower or "single account" in lower:
        account_match = _STANDALONE_ACCOUNT_ID_RE.search(lower)
        return "account", (account_match.group(1) if account_match else None)

    if _contains_analysis_intent(text):
        account_matches = _STANDALONE_ACCOUNT_ID_RE.findall(text)
        if len(account_matches) == 1:
            return "account", account_matches[0]

    if "this account" in lower or "current account" in lower:
        return "current", None

    return "current", None


def _parse_command(prompt: str) -> RouterIntent | None:
    text = prompt.strip()
    if not text.startswith("/"):
        return None

    lower = text.lower()
    if lower.startswith("/retry"):
        return RouterIntent(intent="retry", confidence=1.0)
    if lower.startswith("/rescan"):
        return RouterIntent(intent="rescan", confidence=1.0)
    if lower.startswith("/help"):
        return RouterIntent(intent="help", confidence=1.0)
    if lower.startswith("/athena") and "edit" in lower:
        return RouterIntent(intent="update_athena", confidence=1.0)
    if lower.startswith("/project"):
        name = text[len("/project") :].strip()
        return RouterIntent(intent="set_project", project_name=name or None, confidence=1.0)
    if lower.startswith("/analyze") or lower.startswith("/analyse"):
        match = re.search(r"profile\s*=\s*([A-Za-z0-9._-]+)", text, re.IGNORECASE)
        profile_name = _sanitize_profile_candidate(match.group(1) if match else None)
        target_service = _extract_target_service(text)
        account_scope, target_account_id = _extract_scope_and_account(text)
        return RouterIntent(
            intent="analyze",
            action=ACTION_ID,
            profile_name=profile_name,
            target_service=target_service,
            account_scope=account_scope,
            target_account_id=target_account_id,
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

    if _is_retry_phrase(text):
        return RouterIntent(intent="retry", confidence=0.95)

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

    if _contains_analysis_intent(text):
        profile_name = _infer_profile_from_text(text)
        target_service = _extract_target_service(text)
        account_scope, target_account_id = _extract_scope_and_account(text)
        return RouterIntent(
            intent="analyze",
            action=ACTION_ID,
            profile_name=profile_name,
            target_service=target_service,
            account_scope=account_scope,
            target_account_id=target_account_id,
            confidence=0.78 if profile_name else 0.62,
        )

    target_service = _extract_target_service(text)
    if target_service is not None:
        account_scope, target_account_id = _extract_scope_and_account(text)
        return RouterIntent(
            intent="analyze",
            action=ACTION_ID,
            target_service=target_service,
            account_scope=account_scope,
            target_account_id=target_account_id,
            confidence=0.55,
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
        parsed["profile_name"] = _sanitize_profile_candidate(parsed.get("profile_name")) or _infer_profile_from_text(prompt)
        parsed["target_service"] = parsed.get("target_service") or _extract_target_service(prompt)
        scope, account_id = _extract_scope_and_account(prompt)
        parsed["account_scope"] = parsed.get("account_scope") or scope
        parsed["target_account_id"] = parsed.get("target_account_id") or account_id
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
