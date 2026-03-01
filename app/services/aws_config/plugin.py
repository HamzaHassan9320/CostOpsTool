from __future__ import annotations
from app.core.registry import register
from app.core.types import RunContext, Finding, Evidence
from app.auth.session_factory import make_boto3_session
from app.services.aws_config.collector import collect_config_region
from app.services.aws_config.optimizations.duplicate_global import analyze_duplicate_global

def _list_regions(sess) -> list[str]:
    ec2 = sess.client("ec2")
    return [r["RegionName"] for r in ec2.describe_regions(AllRegions=False)["Regions"]]

@register("aws_config.savings_scan")
class AwsConfigSavingsScan:
    id = "aws_config.savings_scan"

    def run(self, ctx: RunContext) -> list[Finding]:
        sess = make_boto3_session(ctx.profile_name)

        # determine regions
        regions = ctx.regions or _list_regions(sess)

        # collect
        snapshots = [collect_config_region(sess, r) for r in regions]

        # findings
        findings: list[Finding] = []
        findings += analyze_duplicate_global(ctx.account_id, snapshots)

        # you can add more optimizations here:
        # findings += analyze_continuous_vs_daily(...)
        # findings += analyze_rules_eval_hotspots(...)

        # also include an “inventory summary” row if you want
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
            est_monthly_savings_usd=None,
            evidence=[
                Evidence("regions_scanned", len(regions)),
                Evidence("regions", regions),
            ],
        ))

        return findings