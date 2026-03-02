from __future__ import annotations

from app.core.types import Evidence, Finding
from app.services.aws_config.collector import ConfigRegionSnapshot
from app.services.aws_config.costs.athena_cur import RegionCostInput
from app.services.aws_config.optimizations.helpers import apply_edp


def analyze_disable_recorder_only_regions(
    account_id: str | None,
    snapshots: list[ConfigRegionSnapshot],
    region_costs: dict[str, RegionCostInput],
    edp_percent: float,
) -> list[Finding]:
    findings: list[Finding] = []

    for s in snapshots:
        has_recorder = s.recorder is not None
        if not has_recorder:
            continue
        if s.rules_count != 0 or s.conformance_packs_count != 0 or s.aggregators_count != 0:
            continue

        cost = region_costs.get(s.region)
        gross_monthly = cost.total_config_cost_usd if cost is not None else None
        if gross_monthly == 0:
            continue

        savings = apply_edp(gross_monthly, edp_percent)
        confidence = "high" if gross_monthly is not None else "low"
        findings.append(Finding(
            service="aws_config",
            optimization_id="aws_config.disable_recorder_only_region",
            title="Disable recorder in recorder-only region with no consumers",
            account_id=account_id,
            region=s.region,
            severity="high",
            effort="medium",
            risk="high",
            recommendation=(
                "Disable AWS Config recorder in this region only if no governance dependencies exist "
                "and regional compliance requirements are met."
            ),
            confidence=confidence,
            est_monthly_savings_usd=savings[0],
            est_monthly_savings_gross_usd=savings[1],
            est_monthly_savings_net_usd=savings[2],
            est_annual_savings_gross_usd=savings[3],
            est_annual_savings_net_usd=savings[4],
            evidence=[
                Evidence("rules_count", s.rules_count),
                Evidence("conformance_packs_count", s.conformance_packs_count),
                Evidence("aggregators_count", s.aggregators_count),
                Evidence("discovered_total_resources", s.discovered_total_resources),
                Evidence("ci_30d_iam", s.ci_30d_iam),
                Evidence("ci_30d_total", s.ci_30d_total),
                Evidence("region_total_config_cost_usd", cost.total_config_cost_usd if cost else None),
            ],
        ))

    return findings
