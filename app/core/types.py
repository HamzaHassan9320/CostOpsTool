from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal, Optional

@dataclass
class RunContext:
    profile_name: str
    account_id: Optional[str]
    days: int
    regions: list[str]
    athena_database: str
    athena_table: str
    athena_workgroup: str
    athena_output_s3: str
    athena_profile_name: str | None = None
    athena_region: str = "us-east-1"
    requested_by: str | None = None
    progress_callback: Callable[[str, str, float | None], None] | None = None
    aws_session: Any | None = None
    cur_session: Any | None = None
    account_name: str | None = None
    role_name: str | None = None


@dataclass
class NatRecommendationRow:
    account_id: str | None
    region: str
    gateway_name: str
    gateway_id: str
    lookback_duration: str
    bytes_out_to_destination: float
    bytes_out_to_source: float
    active_connections: float
    monthly_cost: float | None


@dataclass
class NatCurCostLine:
    nat_gateway_id: str
    product_region: str
    line_item_usage_type: str
    line_item_operation: str
    net_amortized_usd: float


@dataclass
class AgentRunResult:
    recommendations: list[NatRecommendationRow]
    diagnostics: dict[str, Any]
    sql_used: str | None
    cur_cost_lines: list[NatCurCostLine]
    warnings: list[str]


@dataclass
class ActionRequest:
    # output of the LLM router (safe: no secrets)
    action: str  # single action id for optimization scans
    profile_name: str
    days: int = 30
    regions: list[str] | None = None
    output: Literal["excel", "json", "markdown"] = "excel"
