from __future__ import annotations

from app.core.types import Evidence, Finding
from app.services.aws_config.collector import ConfigRegionSnapshot
from app.services.aws_config.costs.athena_cur import RegionCostInput
from app.services.aws_config.optimizations.helpers import apply_edp

CONTINUOUS_CI_PRICE_USD = 0.003
PERIODIC_CI_PRICE_USD = 0.012


def _recording_frequency(snapshot: ConfigRegionSnapshot) -> str | None:
    recorder = snapshot.recorder or {}
    mode = recorder.get("recordingMode") or {}
    explicit = mode.get("recordingFrequency")
    if explicit:
        return str(explicit)
    if recorder:
        return "CONTINUOUS_RECORDING"
    return None


def analyze_continuous_to_periodic(
    account_id: str | None,
    snapshots: list[ConfigRegionSnapshot],
    region_costs: dict[str, RegionCostInput],
    month_days: int,
    edp_percent: float,
) -> list[Finding]:
    findings: list[Finding] = []

    for s in snapshots:
        frequency = _recording_frequency(s)
        if frequency is None:
            continue
        if frequency != "CONTINUOUS_RECORDING":
            continue

        inventory_n = max(0, s.discovered_resources_excluding_config)
        periodic_ci_est = float(inventory_n * month_days)
        periodic_cost_est = periodic_ci_est * PERIODIC_CI_PRICE_USD

        cost = region_costs.get(s.region)
        if cost is None:
            findings.append(Finding(
                service="aws_config",
                optimization_id="aws_config.continuous_to_periodic",
                title="Evaluate moving from continuous to periodic recording",
                account_id=account_id,
                region=s.region,
                severity="medium",
                effort="medium",
                risk="medium",
                recommendation=(
                    "Insufficient CUR data for this region. Validate CI quantity/cost in CUR, then evaluate "
                    "switching to periodic recording where compliance allows."
                ),
                confidence="low",
                est_monthly_savings_usd=None,
                est_monthly_savings_gross_usd=None,
                est_monthly_savings_net_usd=None,
                est_annual_savings_gross_usd=None,
                est_annual_savings_net_usd=None,
                evidence=[
                    Evidence("recording_frequency", frequency),
                    Evidence("inventory_n_excluding_config", inventory_n),
                    Evidence("month_days", month_days),
                    Evidence("periodic_ci_est", periodic_ci_est),
                    Evidence("periodic_cost_est_usd", periodic_cost_est),
                    Evidence("continuous_ci_price_usd", CONTINUOUS_CI_PRICE_USD),
                    Evidence("periodic_ci_price_usd", PERIODIC_CI_PRICE_USD),
                    Evidence("cur_data_available", False),
                ],
            ))
            continue

        break_even_threshold = 0.25 * float(cost.ci_qty)
        break_even_passes = periodic_ci_est < break_even_threshold if cost.ci_qty > 0 else False
        gross_monthly = float(cost.ci_cost_usd - periodic_cost_est)
        if not break_even_passes or gross_monthly <= 0:
            continue

        savings = apply_edp(gross_monthly, edp_percent)
        findings.append(Finding(
            service="aws_config",
            optimization_id="aws_config.continuous_to_periodic",
            title="Move recorder from continuous to periodic (daily equivalent)",
            account_id=account_id,
            region=s.region,
            severity="high",
            effort="medium",
            risk="high",
            recommendation=(
                "Switch recording to periodic mode only if compliance policy allows reduced change granularity."
            ),
            confidence="high",
            est_monthly_savings_usd=savings[0],
            est_monthly_savings_gross_usd=savings[1],
            est_monthly_savings_net_usd=savings[2],
            est_annual_savings_gross_usd=savings[3],
            est_annual_savings_net_usd=savings[4],
            evidence=[
                Evidence("recording_frequency", frequency),
                Evidence("inventory_n_excluding_config", inventory_n),
                Evidence("month_days", month_days),
                Evidence("periodic_ci_est", periodic_ci_est),
                Evidence("periodic_cost_est_usd", periodic_cost_est),
                Evidence("cur_ci_qty", cost.ci_qty),
                Evidence("cur_ci_cost_usd", cost.ci_cost_usd),
                Evidence("break_even_threshold", break_even_threshold),
                Evidence("break_even_passes", break_even_passes),
            ],
        ))

    return findings
