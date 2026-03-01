from __future__ import annotations
from app.core.types import Finding, Evidence

def analyze_duplicate_global(account_id: str | None, snapshots: list, ) -> list[Finding]:
    findings: list[Finding] = []

    enabled = []
    for s in snapshots:
        rec = s.recorder or {}
        rg = rec.get("recordingGroup") or {}
        if rec and rg.get("includeGlobalResourceTypes") is True:
            enabled.append(s.region)

    if len(enabled) <= 1:
        return findings

    findings.append(Finding(
        service="aws_config",
        optimization_id="aws_config.duplicate_global_resources",
        title="Global resource recording enabled in multiple regions",
        account_id=account_id,
        region=None,
        severity="medium",
        effort="low",
        risk="medium",
        recommendation=(
            "Consider enabling global resource recording in a single chosen region only "
            "and disable includeGlobalResourceTypes elsewhere (subject to compliance requirements)."
        ),
        est_monthly_savings_usd=None,  # attach later when CUR plugged in
        evidence=[
            Evidence("regions_with_includeGlobalResourceTypes", enabled),
            Evidence("count", len(enabled)),
        ],
    ))
    return findings