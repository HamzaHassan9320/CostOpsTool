from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

IntentName = Literal["analyze", "set_project", "update_athena", "rescan", "retry", "help", "chat"]
AccountScope = Literal["current", "all", "account"]


class RouterIntent(BaseModel):
    intent: IntentName = Field(default="chat")
    action: str = Field(default="optimization.run_scan")
    profile_name: str | None = None
    project_name: str | None = None
    target_service: str | None = None
    account_scope: AccountScope = Field(default="current")
    target_account_id: str | None = None
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
