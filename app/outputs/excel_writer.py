from __future__ import annotations

from pathlib import Path
import pandas as pd

from app.core.types import Finding
from app.outputs.report_builder import findings_to_rows


def _summary_rows(findings: list[Finding], account_id: str | None, edp_percent: float | None) -> list[dict]:
    gross_monthly = sum((f.est_monthly_savings_gross_usd or 0.0) for f in findings)
    net_monthly = sum((f.est_monthly_savings_net_usd or 0.0) for f in findings)
    gross_annual = sum((f.est_annual_savings_gross_usd or 0.0) for f in findings)
    net_annual = sum((f.est_annual_savings_net_usd or 0.0) for f in findings)
    return [
        {"metric": "account_id", "value": account_id},
        {"metric": "edp_percent", "value": edp_percent},
        {"metric": "optimization_rows", "value": len(findings)},
        {"metric": "monthly_savings_gross_usd_total", "value": gross_monthly},
        {"metric": "monthly_savings_net_usd_total", "value": net_monthly},
        {"metric": "annual_savings_gross_usd_total", "value": gross_annual},
        {"metric": "annual_savings_net_usd_total", "value": net_annual},
        {"metric": "generated_utc", "value": pd.Timestamp.utcnow().isoformat()},
    ]


def _validation_sql(account_id: str | None) -> str:
    safe_account = (account_id or "").replace("'", "''")
    return f"""
SELECT
  date(line_item_usage_start_date) AS day,
  product_region AS region,
  SUM(line_item_usage_amount) AS cir_qty,
  SUM(line_item_unblended_cost) AS cir_cost_usd
FROM cudos01
WHERE line_item_line_item_type='Usage'
  AND line_item_usage_account_id='{safe_account}'
  AND line_item_usage_start_date >= (current_date - INTERVAL '30' DAY)
  AND line_item_usage_type LIKE '%ConfigurationItemRecorded%'
GROUP BY 1,2
ORDER BY day, region;
""".strip()


def write_excel(
    findings: list[Finding],
    out_path: str,
    account_id: str | None = None,
    edp_percent: float | None = None,
    region_cost_rows: list[dict] | None = None,
    validation_sql: str | None = None,
) -> str:
    out = Path(out_path)
    opportunities = [f for f in findings if f.optimization_id != "aws_config.inventory_summary"]
    opportunities_df = pd.DataFrame(findings_to_rows(opportunities))
    if not opportunities_df.empty:
        opportunities_df = opportunities_df.sort_values(
            by=["service", "severity", "optimization_id", "region"],
            ascending=[True, False, True, True],
        )

    summary_df = pd.DataFrame(_summary_rows(opportunities, account_id=account_id, edp_percent=edp_percent))
    costs_df = pd.DataFrame(region_cost_rows or [])
    validation_df = pd.DataFrame(
        [{"name": "config_ci_validation_last_30_days", "sql": validation_sql or _validation_sql(account_id)}]
    )

    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        summary_df.to_excel(xw, sheet_name="Summary", index=False)
        opportunities_df.to_excel(xw, sheet_name="Opportunities", index=False)
        costs_df.to_excel(xw, sheet_name="Region_Cost_Inputs", index=False)
        validation_df.to_excel(xw, sheet_name="Validation_SQL", index=False)

    return str(out)
