from __future__ import annotations
from app.core.types import Finding, Evidence
from app.services.aws_config.collector import ConfigRegionSnapshot
from app.services.aws_config.costs.athena_cur import RegionCostInput
from app.services.aws_config.optimizations.helpers import apply_edp


def _primary_region(enabled_regions: list[str], region_costs: dict[str, RegionCostInput]) -> str:
    best_region = None
    best_cost = -1.0
    for region in enabled_regions:
        ci_cost = region_costs.get(region).ci_cost_usd if region in region_costs else None
        if ci_cost is None:
            continue
        if ci_cost > best_cost:
            best_cost = ci_cost
            best_region = region
    if best_region:
        return best_region
    return sorted(enabled_regions)[0]


def analyze_duplicate_global(
    account_id: str | None,
    snapshots: list[ConfigRegionSnapshot],
    region_costs: dict[str, RegionCostInput],
    edp_percent: float,
) -> list[Finding]:
    findings: list[Finding] = []

    enabled: list[ConfigRegionSnapshot] = []
    for s in snapshots:
        rec = s.recorder or {}
        rg = rec.get("recordingGroup") or {}
        if rec and rg.get("includeGlobalResourceTypes") is True:
            enabled.append(s)

    if len(enabled) <= 1:
        return findings

    enabled_regions = [s.region for s in enabled]
    primary_region = _primary_region(enabled_regions, region_costs)
    enabled_ci_costs = [
        region_costs[r].ci_cost_usd
        for r in enabled_regions
        if r in region_costs
    ]
    # Conservative lower bound: use the lowest known CI cost across all duplicate-enabled regions.
    conservative_ci_cost = min(enabled_ci_costs) if enabled_ci_costs else None

    for s in enabled:
        if s.region == primary_region:
            continue
        cost = region_costs.get(s.region)
        iam_share = (s.ci_30d_iam / s.ci_30d_total) if s.ci_30d_total > 0 else None
        ci_cost_for_estimate = conservative_ci_cost
        if ci_cost_for_estimate is None and cost is not None:
            ci_cost_for_estimate = cost.ci_cost_usd
        gross_monthly = (iam_share * ci_cost_for_estimate) if (iam_share is not None and ci_cost_for_estimate is not None) else None
        savings = apply_edp(gross_monthly, edp_percent)
        confidence = "medium" if gross_monthly is not None else "low"
        findings.append(Finding(
            service="aws_config",
            optimization_id="aws_config.duplicate_global_resources",
            title="Disable global resource recording in non-primary region",
            account_id=account_id,
            region=s.region,
            severity="medium",
            effort="low",
            risk="medium",
            recommendation=(
                "Keep includeGlobalResourceTypes enabled only in the primary region and disable it in this region "
                "(subject to compliance requirements)."
            ),
            confidence=confidence,
            est_monthly_savings_usd=savings[0],
            est_monthly_savings_gross_usd=savings[1],
            est_monthly_savings_net_usd=savings[2],
            est_annual_savings_gross_usd=savings[3],
            est_annual_savings_net_usd=savings[4],
            evidence=[
                Evidence("primary_region", primary_region),
                Evidence("regions_with_includeGlobalResourceTypes", enabled_regions),
                Evidence("region_ci_30d_total", s.ci_30d_total),
                Evidence("region_ci_30d_iam", s.ci_30d_iam),
                Evidence("iam_share", iam_share),
                Evidence("region_ci_cost_usd", cost.ci_cost_usd if cost else None),
                Evidence("ci_cost_for_estimate_usd", ci_cost_for_estimate),
                Evidence("estimate_mode", "conservative_lower_bound_across_removable_regions"),
            ],
        ))
    return findings
