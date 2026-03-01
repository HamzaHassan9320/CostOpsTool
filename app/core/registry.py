from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Dict, Protocol

from app.core.types import RunContext, Finding, ActionRequest

class Plugin(Protocol):
    id: str
    def run(self, ctx: RunContext) -> list[Finding]: ...

_REGISTRY: Dict[str, Plugin] = {}

def register(action_id: str):
    def _decorator(cls):
        _REGISTRY[action_id] = cls()
        return cls
    return _decorator

def list_actions() -> list[str]:
    return sorted(_REGISTRY.keys())

def run_action(req: ActionRequest, build_ctx: Callable[[ActionRequest], RunContext]) -> list[Finding]:
    if req.action not in _REGISTRY:
        raise ValueError(f"Unknown action: {req.action}. Available: {list_actions()}")
    ctx = build_ctx(req)
    return _REGISTRY[req.action].run(ctx)