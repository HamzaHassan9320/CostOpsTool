from __future__ import annotations
import re

from app.auth.session_factory import make_boto3_session
from app.core.registry import register
from app.core.types import Evidence, Finding, RunContext
from app.services.aws_config.collector import ConfigRegionSnapshot, collect_config_region
from app.services.aws_config.costs.athena_cur import CurCostResult, get_last_full_month_config_costs_by_region
from app.services.aws_config.optimizations.continuous_to_periodic import analyze_continuous_to_periodic
from app.services.aws_config.optimizations.disable_recorder_only_regions import analyze_disable_recorder_only_regions
from app.services.aws_config.optimizations.duplicate_global import analyze_duplicate_global
from app.services.aws_config.optimizations.exclude_resource_compliance import analyze_exclude_resource_compliance


def _list_regions(sess) -> list[str]:
    ec2 = sess.client("ec2")
    return [r["RegionName"] for r in ec2.describe_regions(AllRegions=False)["Regions"]]


def _is_recorder_enabled(snapshot: ConfigRegionSnapshot) -> bool:
    if snapshot.recorder is None:
        return False
    status = snapshot.recorder_status or {}
    recording = status.get("recording")
    if recording is None:
        return True
    return bool(recording)


def _regions_with_enabled_recorder(sess, regions: list[str]) -> list[str]:
    enabled: list[str] = []
    for region in regions:
        try:
            cfg = sess.client("config", region_name=region)
            recs = cfg.describe_configuration_recorders().get("ConfigurationRecorders", [])
            if not recs:
                continue
            statuses = cfg.describe_configuration_recorder_status().get("ConfigurationRecordersStatus", [])
            status = statuses[0] if statuses else {}
            recording = status.get("recording")
            if recording is None or bool(recording):
                enabled.append(region)
        except Exception:
            # Skip regions that are disabled/unavailable or where API calls are blocked.
            continue
    return enabled


def _pick_regions(
    requested_regions: list[str],
    cur_result: CurCostResult | None,
    sess,
) -> list[str]:
    if requested_regions:
        return requested_regions
    if cur_result is not None:
        cur_regions = [
            region
            for region, row in cur_result.rows.items()
            if row.total_config_cost_usd > 0 and re.match(r"^[a-z]{2}(-gov)?-[a-z]+-\d$", region)
        ]
        if cur_regions:
            return sorted(cur_regions)
    all_regions = _list_regions(sess)
    return _regions_with_enabled_recorder(sess, all_regions)


def _region_cost_rows(cur_result: CurCostResult | None) -> list[dict]:
    if cur_result is None:
        return []
    rows: list[dict] = []
    for region, item in sorted(cur_result.rows.items()):
        rows.append({
            "month_start": item.month_start,
            "region": region,
            "ci_qty": item.ci_qty,
            "ci_cost_usd": item.ci_cost_usd,
            "eval_qty": item.eval_qty,
            "eval_cost_usd": item.eval_cost_usd,
            "total_config_cost_usd": item.total_config_cost_usd,
        })
    return rows


def _safe_float(value: float | None) -> float:
    if value is None:
        return 0.0
    return float(value)


def _apply_disable_recorder_overlap_adjustment(findings: list[Finding]) -> None:
    """
    Prevent double counting when a full region recorder shutdown recommendation coexists
    with other same-region recommendations.
    """
    for f in findings:
        if f.optimization_id != "aws_config.disable_recorder_only_region":
            continue
        if not f.region:
            continue

        overlap_rows = [
            other
            for other in findings
            if other is not f
            and other.region == f.region
            and other.optimization_id not in {"aws_config.inventory_summary", "aws_config.disable_recorder_only_region"}
        ]
        if not overlap_rows:
            continue

        overlap_gross_monthly = sum(_safe_float(x.est_monthly_savings_gross_usd) for x in overlap_rows)
        overlap_net_monthly = sum(_safe_float(x.est_monthly_savings_net_usd) for x in overlap_rows)
        overlap_gross_annual = sum(_safe_float(x.est_annual_savings_gross_usd) for x in overlap_rows)
        overlap_net_annual = sum(_safe_float(x.est_annual_savings_net_usd) for x in overlap_rows)

        current_gross_monthly = _safe_float(f.est_monthly_savings_gross_usd)
        current_net_monthly = _safe_float(f.est_monthly_savings_net_usd)
        current_gross_annual = _safe_float(f.est_annual_savings_gross_usd)
        current_net_annual = _safe_float(f.est_annual_savings_net_usd)

        adjusted_gross_monthly = max(0.0, current_gross_monthly - overlap_gross_monthly)
        adjusted_net_monthly = max(0.0, current_net_monthly - overlap_net_monthly)
        adjusted_gross_annual = max(0.0, current_gross_annual - overlap_gross_annual)
        adjusted_net_annual = max(0.0, current_net_annual - overlap_net_annual)

        f.est_monthly_savings_usd = adjusted_gross_monthly
        f.est_monthly_savings_gross_usd = adjusted_gross_monthly
        f.est_monthly_savings_net_usd = adjusted_net_monthly
        f.est_annual_savings_gross_usd = adjusted_gross_annual
        f.est_annual_savings_net_usd = adjusted_net_annual
        f.evidence.append(Evidence("overlap_adjustment_applied", True))
        f.evidence.append(Evidence("overlap_with_optimization_ids", [x.optimization_id for x in overlap_rows]))
        f.evidence.append(Evidence("overlap_gross_monthly_usd", overlap_gross_monthly))
        f.evidence.append(Evidence("overlap_net_monthly_usd", overlap_net_monthly))


@register("aws_config.savings_scan")
class AwsConfigSavingsScan:
    id = "aws_config.savings_scan"

    def run(self, ctx: RunContext) -> list[Finding]:
        sess = make_boto3_session(ctx.profile_name)
        cur_profile = (ctx.athena_profile_name or "").strip() or ctx.profile_name
        cur_sess = make_boto3_session(cur_profile)

        cur_result = None
        cur_error = None
        cur_warning = None
        if ctx.account_id and ctx.athena_database and ctx.athena_table and ctx.athena_workgroup and ctx.athena_output_s3:
            try:
                cur_result = get_last_full_month_config_costs_by_region(
                    sess=cur_sess,
                    account_id=ctx.account_id,
                    database=ctx.athena_database,
                    table=ctx.athena_table,
                    workgroup=ctx.athena_workgroup,
                    output_s3=ctx.athena_output_s3,
                    athena_region=ctx.athena_region,
                )
                if cur_result.warning:
                    cur_warning = cur_result.warning
            except Exception as ex:
                cur_error = str(ex)

        regions = _pick_regions(ctx.regions, cur_result, sess)
        snapshots = [collect_config_region(sess, r) for r in regions]
        region_costs = cur_result.rows if cur_result else {}
        month_days = cur_result.month_days if cur_result else 30

        findings: list[Finding] = []
        findings += analyze_duplicate_global(
            account_id=ctx.account_id,
            snapshots=snapshots,
            region_costs=region_costs,
            edp_percent=ctx.edp_percent,
        )
        findings += analyze_continuous_to_periodic(
            account_id=ctx.account_id,
            snapshots=snapshots,
            region_costs=region_costs,
            month_days=month_days,
            edp_percent=ctx.edp_percent,
        )
        findings += analyze_exclude_resource_compliance(
            account_id=ctx.account_id,
            snapshots=snapshots,
            region_costs=region_costs,
            edp_percent=ctx.edp_percent,
        )
        findings += analyze_disable_recorder_only_regions(
            account_id=ctx.account_id,
            snapshots=snapshots,
            region_costs=region_costs,
            edp_percent=ctx.edp_percent,
        )
        _apply_disable_recorder_overlap_adjustment(findings)

        findings.append(Finding(
            service="aws_config",
            optimization_id="aws_config.inventory_summary",
            title="AWS Config inventory summary",
            account_id=ctx.account_id,
            region=None,
            severity="low",
            effort="low",
            risk="low",
            recommendation="N/A",
            confidence="high",
            est_monthly_savings_usd=None,
            est_monthly_savings_gross_usd=None,
            est_monthly_savings_net_usd=None,
            est_annual_savings_gross_usd=None,
            est_annual_savings_net_usd=None,
            evidence=[
                Evidence("regions_scanned", len(regions)),
                Evidence("regions", regions),
                Evidence("cur_region_count", len(region_costs)),
                Evidence("cur_available", cur_result is not None),
                Evidence("cur_error", cur_error),
                Evidence("cur_warning", cur_warning),
                Evidence("cur_fallback_used", cur_result.fallback_used if cur_result else False),
                Evidence("cur_month_start", str(cur_result.month_start) if cur_result else None),
                Evidence("cur_query_profile", cur_profile),
                Evidence("cur_query_region", ctx.athena_region),
                Evidence("month_days_used_for_periodic_estimate", month_days),
                Evidence("cur_query_sql", cur_result.sql if cur_result else None),
                Evidence("region_cost_inputs", _region_cost_rows(cur_result)),
            ],
        ))

        return findings
