from __future__ import annotations

from app.core.types import Evidence, Finding
from app.services.aws_config.collector import ConfigRegionSnapshot
from app.services.aws_config.costs.athena_cur import RegionCostInput
from app.services.aws_config.optimizations.helpers import apply_edp


def analyze_exclude_resource_compliance(
    account_id: str | None,
    snapshots: list[ConfigRegionSnapshot],
    region_costs: dict[str, RegionCostInput],
    edp_percent: float,
) -> list[Finding]:
    findings: list[Finding] = []

    for s in snapshots:
        if s.ci_30d_total <= 0 or s.ci_30d_resource_compliance <= 0:
            continue
        share = s.ci_30d_resource_compliance / s.ci_30d_total
        severity = "high" if share >= 0.5 else "medium"

        cost = region_costs.get(s.region)
        gross_monthly = (share * cost.ci_cost_usd) if cost is not None else None
        savings = apply_edp(gross_monthly, edp_percent)
        confidence = "medium" if gross_monthly is not None else "low"

        findings.append(Finding(
            service="aws_config",
            optimization_id="aws_config.exclude_resource_compliance",
            title="Exclude AWS::Config::ResourceCompliance from recording",
            account_id=account_id,
            region=s.region,
            severity=severity,
            effort="low",
            risk="medium",
            recommendation=(
                "Consider excluding AWS::Config::ResourceCompliance where allowed. Savings estimate is a proxy "
                "based on 30-day resource-type share and must be validated in CUR after change."
            ),
            confidence=confidence,
            est_monthly_savings_usd=savings[0],
            est_monthly_savings_gross_usd=savings[1],
            est_monthly_savings_net_usd=savings[2],
            est_annual_savings_gross_usd=savings[3],
            est_annual_savings_net_usd=savings[4],
            evidence=[
                Evidence("ci_30d_total", s.ci_30d_total),
                Evidence("ci_30d_resource_compliance", s.ci_30d_resource_compliance),
                Evidence("resource_compliance_share_proxy", share),
                Evidence("cur_ci_cost_usd", cost.ci_cost_usd if cost else None),
                Evidence("estimate_type", "proxy"),
            ],
        ))

    return findings
