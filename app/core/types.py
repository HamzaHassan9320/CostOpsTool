from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Literal, Optional

Severity = Literal["low", "medium", "high"]
Effort = Literal["low", "medium", "high"]
Risk = Literal["low", "medium", "high"]
Confidence = Literal["low", "medium", "high"]

@dataclass
class RunContext:
    profile_name: str
    account_id: Optional[str]
    days: int
    regions: list[str]
    edp_percent: float
    athena_database: str
    athena_table: str
    athena_workgroup: str
    athena_output_s3: str
    athena_profile_name: str | None = None
    athena_region: str = "us-east-1"
    requested_by: str | None = None

@dataclass
class Evidence:
    key: str
    value: Any
    note: str | None = None

@dataclass
class Finding:
    service: str
    optimization_id: str
    title: str
    account_id: str | None
    region: str | None
    severity: Severity
    effort: Effort
    risk: Risk
    recommendation: str
    confidence: Confidence
    est_monthly_savings_usd: float | None
    est_monthly_savings_gross_usd: float | None
    est_monthly_savings_net_usd: float | None
    est_annual_savings_gross_usd: float | None
    est_annual_savings_net_usd: float | None
    evidence: list[Evidence]

    def to_dict(self) -> dict:
        d = asdict(self)
        d["evidence"] = [asdict(e) for e in self.evidence]
        return d

@dataclass
class ActionRequest:
    # output of the LLM router (safe: no secrets)
    action: str                 # e.g. "aws_config.savings_scan"
    profile_name: str           # local AWS profile to use
    days: int = 30
    regions: list[str] | None = None
    output: Literal["excel", "json", "markdown"] = "excel"
