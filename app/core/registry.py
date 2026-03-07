from __future__ import annotations

from typing import Callable, Dict, Protocol

from app.core.types import ActionRequest, AgentRunResult, RunContext


class Plugin(Protocol):
    id: str

    def run(self, ctx: RunContext) -> AgentRunResult:
        ...


_REGISTRY: Dict[str, Plugin] = {}


def register(action_id: str):
    def _decorator(cls):
        _REGISTRY[action_id] = cls()
        return cls

    return _decorator


def list_actions() -> list[str]:
    return sorted(_REGISTRY.keys())


def run_action(req: ActionRequest, build_ctx: Callable[[ActionRequest], RunContext]) -> AgentRunResult:
    if req.action not in _REGISTRY:
        raise ValueError(f"Unknown action: {req.action}. Available: {list_actions()}")
    ctx = build_ctx(req)
    return _REGISTRY[req.action].run(ctx)
