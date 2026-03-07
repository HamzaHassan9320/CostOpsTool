from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
import os
import re
import time
from typing import Any

from app.core.types import NatCurCostLine


@dataclass
class NatGatewayCurCostResult:
    monthly_cost_by_nat_id: dict[str, float]
    eip_monthly_price_per_eip: float | None
    lines: list[NatCurCostLine]
    sql: str
    month_start: date
    month_days: int
    warning: str | None = None


def _quote_identifier(value: str) -> str:
    safe = value.replace('"', '""')
    return f'"{safe}"'


def _table_ref(database: str, table: str) -> str:
    db = (database or "").strip()
    tbl = (table or "").strip()
    if not tbl:
        raise ValueError("Athena table must be provided.")
    if "." in tbl and not db:
        left, right = tbl.split(".", 1)
        if left and right:
            return f"{_quote_identifier(left)}.{_quote_identifier(right)}"
    if not db:
        raise ValueError("Athena database must be provided when table is not fully qualified.")
    return f"{_quote_identifier(db)}.{_quote_identifier(tbl)}"


def _last_full_month_start(today: date) -> date:
    first_of_current = date(today.year, today.month, 1)
    return (first_of_current - timedelta(days=1)).replace(day=1)


def _to_float(raw: Any) -> float:
    if raw is None:
        return 0.0
    if isinstance(raw, (int, float, Decimal)):
        return float(raw)
    text = str(raw).strip()
    if not text:
        return 0.0
    return float(text)


def _validate_nat_ids(nat_gateway_ids: list[str]) -> list[str]:
    safe: list[str] = []
    for raw in nat_gateway_ids:
        candidate = (raw or "").strip().lower()
        if not candidate:
            continue
        if re.match(r"^nat-[0-9a-f]+$", candidate):
            safe.append(candidate)
    return sorted(set(safe))


def _build_sql(table_ref: str, escaped_account: str, nat_gateway_ids: list[str]) -> str:
    if not nat_gateway_ids:
        return "SELECT '' AS nat_gateway_id, '' AS product_region, '' AS line_item_usage_type, '' AS line_item_operation, CAST(0 AS double) AS net_amortized_usd WHERE 1=0"

    in_list = ",\n  ".join([f"'{nat_id}'" for nat_id in nat_gateway_ids])
    return f"""
WITH nat_lines AS (
  SELECT
    regexp_extract(lower(coalesce(line_item_resource_id,'')), 'nat-[0-9a-f]+') AS nat_gateway_id,
    product_region,
    line_item_usage_type,
    line_item_operation,
    CAST(
      CASE
        WHEN line_item_line_item_type = 'SavingsPlanRecurringFee' THEN
          (
            savings_plan_total_commitment_to_date - savings_plan_used_commitment
          ) * COALESCE(
                COALESCE(savings_plan_net_amortized_upfront_commitment_for_billing_period,
                         savings_plan_amortized_upfront_commitment_for_billing_period)
                / NULLIF(savings_plan_amortized_upfront_commitment_for_billing_period, 0),
                1
              )
        WHEN line_item_line_item_type = 'RIFee' THEN
          COALESCE(
            reservation_net_unused_amortized_upfront_fee_for_billing_period + reservation_net_unused_recurring_fee,
            reservation_unused_amortized_upfront_fee_for_billing_period + reservation_unused_recurring_fee
          )
        WHEN line_item_line_item_type = 'SavingsPlanCoveredUsage' THEN
          COALESCE(savings_plan_net_savings_plan_effective_cost, savings_plan_savings_plan_effective_cost)
        WHEN line_item_line_item_type = 'DiscountedUsage' THEN
          COALESCE(reservation_net_effective_cost, reservation_effective_cost)
        WHEN line_item_line_item_type = 'Fee'
             AND coalesce(reservation_reservation_a_r_n, '') = '' THEN
          COALESCE(line_item_net_unblended_cost, line_item_unblended_cost)
        WHEN line_item_line_item_type IN ('Usage', 'Tax', 'Credit', 'Refund') THEN
          COALESCE(line_item_net_unblended_cost, line_item_unblended_cost)
        ELSE 0
      END
    AS double) AS net_amortized_usd
  FROM {table_ref}
  WHERE line_item_usage_account_id = '{escaped_account}'
    AND date_trunc('month', line_item_usage_start_date) = date_trunc('month', current_date) - INTERVAL '1' MONTH
    AND line_item_line_item_type <> 'Tax'
    AND lower(coalesce(line_item_usage_type,'')) LIKE '%natgateway-hours%'
)
SELECT
  nat_gateway_id,
  product_region,
  line_item_usage_type,
  line_item_operation,
  SUM(net_amortized_usd) AS net_amortized_usd
FROM nat_lines
WHERE nat_gateway_id IN (
  {in_list}
)
GROUP BY 1,2,3,4
ORDER BY net_amortized_usd DESC
""".strip()


def _build_eip_sql(table_ref: str, escaped_account: str) -> str:
    return f"""
WITH eip_rate AS (
  SELECT
    SUM(CAST(COALESCE(line_item_usage_amount, 0) AS double)) AS total_hours,
    SUM(CAST(COALESCE(line_item_net_unblended_cost, line_item_unblended_cost) AS double)) AS total_net_usd
  FROM {table_ref}
  WHERE line_item_usage_account_id = '{escaped_account}'
    AND date_trunc('month', line_item_usage_start_date) = date_trunc('month', current_date) - INTERVAL '1' MONTH
    AND line_item_line_item_type <> 'Tax'
    AND lower(coalesce(line_item_usage_type,'')) LIKE '%publicipv4%'
)
SELECT
  total_hours,
  total_net_usd,
  total_net_usd / NULLIF(total_hours, 0) AS effective_usd_per_hour,
  (total_net_usd / NULLIF(total_hours, 0))
    * 24
    * date_diff(
        'day',
        date_trunc('month', current_date) - INTERVAL '1' MONTH,
        date_trunc('month', current_date)
      ) AS effective_monthly_price_per_eip
FROM eip_rate
""".strip()


def _run_athena_query(
    sess,
    sql: str,
    workgroup: str,
    output_s3: str,
    athena_region: str | None = None,
    poll_seconds: float = 1.0,
    timeout_seconds: int = 420,
) -> list[dict[str, str]]:
    timeout_override = os.getenv("ATHENA_QUERY_TIMEOUT_SECONDS", "").strip()
    if timeout_override:
        try:
            timeout_seconds = max(30, int(timeout_override))
        except Exception:
            timeout_seconds = timeout_seconds
    athena = sess.client("athena", region_name=athena_region) if athena_region else sess.client("athena")
    response = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output_s3},
    )
    query_execution_id = response["QueryExecutionId"]

    started = time.time()
    while True:
        status_resp = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status_resp["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "CANCELLED"}:
            reason = status_resp["QueryExecution"]["Status"].get("StateChangeReason", state)
            raise RuntimeError(f"Athena query failed: {reason}")
        if (time.time() - started) > timeout_seconds:
            raise TimeoutError("Athena query timed out while waiting for completion.")
        time.sleep(poll_seconds)

    paginator = athena.get_paginator("get_query_results")
    pages = paginator.paginate(QueryExecutionId=query_execution_id)
    rows: list[dict[str, str]] = []
    headers: list[str] = []
    for page in pages:
        result_rows = page.get("ResultSet", {}).get("Rows", [])
        for index, row in enumerate(result_rows):
            values = [c.get("VarCharValue", "") for c in row.get("Data", [])]
            if not headers:
                headers = values
                continue
            if index == 0 and values == headers:
                continue
            rows.append(dict(zip(headers, values)))
    return rows


def _parse_rows(raw_rows: list[dict[str, str]]) -> tuple[dict[str, float], list[NatCurCostLine]]:
    monthly_by_nat: dict[str, float] = {}
    lines: list[NatCurCostLine] = []
    for row in raw_rows:
        nat_id = (row.get("nat_gateway_id") or "").strip().lower()
        if not nat_id:
            continue
        amount = _to_float(row.get("net_amortized_usd"))
        monthly_by_nat[nat_id] = monthly_by_nat.get(nat_id, 0.0) + amount
        lines.append(
            NatCurCostLine(
                nat_gateway_id=nat_id,
                product_region=(row.get("product_region") or "").strip(),
                line_item_usage_type=(row.get("line_item_usage_type") or "").strip(),
                line_item_operation=(row.get("line_item_operation") or "").strip(),
                net_amortized_usd=amount,
            )
        )
    return monthly_by_nat, lines


def _parse_eip_monthly_price(raw_rows: list[dict[str, str]]) -> float | None:
    if not raw_rows:
        return None
    row = raw_rows[0]
    total_hours = _to_float(row.get("total_hours"))
    if total_hours <= 0:
        return None
    return _to_float(row.get("effective_monthly_price_per_eip"))


def get_last_full_month_nat_gateway_net_amortized_costs_by_ids(
    sess,
    account_id: str,
    database: str,
    table: str,
    workgroup: str,
    output_s3: str,
    nat_gateway_ids: list[str],
    athena_region: str | None = "us-east-1",
) -> NatGatewayCurCostResult:
    month_start = _last_full_month_start(date.today())
    month_days = monthrange(month_start.year, month_start.month)[1]
    safe_ids = _validate_nat_ids(nat_gateway_ids)
    table_ref = _table_ref(database=database, table=table)
    escaped_account = account_id.replace("'", "''")

    if not safe_ids:
        return NatGatewayCurCostResult(
            monthly_cost_by_nat_id={},
            eip_monthly_price_per_eip=None,
            lines=[],
            sql="",
            month_start=month_start,
            month_days=month_days,
            warning="No NAT gateway IDs were provided for CUR lookup.",
        )

    sql = _build_sql(table_ref=table_ref, escaped_account=escaped_account, nat_gateway_ids=safe_ids)
    eip_sql = _build_eip_sql(table_ref=table_ref, escaped_account=escaped_account)
    raw_rows = _run_athena_query(
        sess,
        sql=sql,
        workgroup=workgroup,
        output_s3=output_s3,
        athena_region=athena_region,
    )
    eip_warning = None
    eip_monthly_price_per_eip: float | None = None
    try:
        eip_rows = _run_athena_query(
            sess,
            sql=eip_sql,
            workgroup=workgroup,
            output_s3=output_s3,
            athena_region=athena_region,
        )
        eip_monthly_price_per_eip = _parse_eip_monthly_price(eip_rows)
    except Exception as ex:
        eip_warning = f"EIP price query failed: {ex}"

    monthly_by_nat, lines = _parse_rows(raw_rows)
    warnings: list[str] = []
    if not lines:
        warnings.append("No NAT Gateway CUR rows found in last full month for the selected gateways.")
    if eip_warning:
        warnings.append(eip_warning)
    warning = " ".join(warnings) if warnings else None

    return NatGatewayCurCostResult(
        monthly_cost_by_nat_id=monthly_by_nat,
        eip_monthly_price_per_eip=eip_monthly_price_per_eip,
        lines=lines,
        sql=sql,
        month_start=month_start,
        month_days=month_days,
        warning=warning,
    )
