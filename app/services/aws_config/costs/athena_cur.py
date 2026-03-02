from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from calendar import monthrange
from decimal import Decimal
import time
import re
from typing import Any


@dataclass
class RegionCostInput:
    month_start: str
    region: str
    ci_qty: float
    ci_cost_usd: float
    eval_qty: float
    eval_cost_usd: float
    total_config_cost_usd: float


@dataclass
class CurCostResult:
    rows: dict[str, RegionCostInput]
    sql: str
    month_start: date
    month_days: int
    fallback_used: bool = False
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


def _to_float(raw: Any) -> float:
    if raw is None:
        return 0.0
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, Decimal):
        return float(raw)
    text = str(raw).strip()
    if not text:
        return 0.0
    return float(text)


def _last_full_month_start(today: date) -> date:
    first_of_current = date(today.year, today.month, 1)
    return (first_of_current - timedelta(days=1)).replace(day=1)


def _latest_available_month_sql(table_ref: str, escaped_account: str) -> str:
    return f"""
(
  SELECT COALESCE(
    MAX(CASE WHEN m < date_trunc('month', current_date) THEN m END),
    MAX(m)
  )
  FROM (
    SELECT date_trunc('month', line_item_usage_start_date) AS m
    FROM {table_ref}
    WHERE line_item_line_item_type = 'Usage'
      AND line_item_usage_account_id = '{escaped_account}'
      AND (
        line_item_usage_type LIKE '%ConfigurationItemRecorded%'
        OR line_item_usage_type LIKE '%ConfigRuleEvaluations%'
      )
    GROUP BY 1
  ) t
)
""".strip()


def _build_sql(table_ref: str, escaped_account: str, month_selector_sql: str) -> str:
    return f"""
SELECT
  date_trunc('month', line_item_usage_start_date) AS month_start,
  product_region AS region,
  SUM(CASE WHEN line_item_usage_type LIKE '%ConfigurationItemRecorded%' THEN line_item_usage_amount ELSE 0 END) AS ci_qty,
  SUM(CASE WHEN line_item_usage_type LIKE '%ConfigurationItemRecorded%' THEN line_item_unblended_cost ELSE 0 END) AS ci_cost_usd,
  SUM(CASE WHEN line_item_usage_type LIKE '%ConfigRuleEvaluations%' THEN line_item_usage_amount ELSE 0 END) AS eval_qty,
  SUM(CASE WHEN line_item_usage_type LIKE '%ConfigRuleEvaluations%' THEN line_item_unblended_cost ELSE 0 END) AS eval_cost_usd,
  SUM(CASE
        WHEN line_item_product_code = 'AWSConfig'
          OR product_product_name = 'AWS Config'
          OR line_item_usage_type LIKE '%ConfigurationItemRecorded%'
          OR line_item_usage_type LIKE '%ConfigRuleEvaluations%'
        THEN line_item_unblended_cost
        ELSE 0
      END) AS total_config_cost_usd
FROM {table_ref}
WHERE line_item_line_item_type = 'Usage'
  AND line_item_usage_account_id = '{escaped_account}'
  AND date_trunc('month', line_item_usage_start_date) = {month_selector_sql}
GROUP BY 1, 2
HAVING SUM(CASE
            WHEN line_item_product_code = 'AWSConfig'
              OR product_product_name = 'AWS Config'
              OR line_item_usage_type LIKE '%ConfigurationItemRecorded%'
              OR line_item_usage_type LIKE '%ConfigRuleEvaluations%'
            THEN line_item_unblended_cost
            ELSE 0
          END) > 0
ORDER BY total_config_cost_usd DESC
""".strip()


_REGION_NAME_TO_CODE = {
    "eu (ireland)": "eu-west-1",
    "eu (london)": "eu-west-2",
    "eu (stockholm)": "eu-north-1",
    "eu (frankfurt)": "eu-central-1",
    "us east (n. virginia)": "us-east-1",
    "us east (ohio)": "us-east-2",
    "us west (n. california)": "us-west-1",
    "us west (oregon)": "us-west-2",
    "asia pacific (tokyo)": "ap-northeast-1",
    "asia pacific (seoul)": "ap-northeast-2",
    "asia pacific (osaka)": "ap-northeast-3",
    "asia pacific (mumbai)": "ap-south-1",
    "asia pacific (singapore)": "ap-southeast-1",
    "asia pacific (sydney)": "ap-southeast-2",
    "asia pacific (jakarta)": "ap-southeast-3",
    "asia pacific (hong kong)": "ap-east-1",
    "canada (central)": "ca-central-1",
    "south america (sao paulo)": "sa-east-1",
    "middle east (bahrain)": "me-south-1",
    "africa (cape town)": "af-south-1",
}

_REGION_ALIAS_CONTAINS = {
    "ireland": "eu-west-1",
    "london": "eu-west-2",
    "stockholm": "eu-north-1",
    "frankfurt": "eu-central-1",
    "n. virginia": "us-east-1",
    "ohio": "us-east-2",
    "oregon": "us-west-2",
    "n. california": "us-west-1",
    "tokyo": "ap-northeast-1",
    "seoul": "ap-northeast-2",
    "osaka": "ap-northeast-3",
    "mumbai": "ap-south-1",
    "singapore": "ap-southeast-1",
    "sydney": "ap-southeast-2",
    "jakarta": "ap-southeast-3",
    "hong kong": "ap-east-1",
    "bahrain": "me-south-1",
    "cape town": "af-south-1",
}


def _normalize_region(raw_region: str | None) -> str | None:
    if not raw_region:
        return None
    region = raw_region.strip()
    if not region:
        return None
    lower = region.lower()
    if re.match(r"^[a-z]{2}(-gov)?-[a-z]+-\d$", lower):
        return lower
    if lower in _REGION_NAME_TO_CODE:
        return _REGION_NAME_TO_CODE[lower]
    for token, code in _REGION_ALIAS_CONTAINS.items():
        if token in lower:
            return code
    # Keep unknown region text for debugging visibility rather than dropping rows.
    return lower


def _parse_month_start(text: str | None, fallback: date) -> date:
    if not text:
        return fallback
    value = text.strip()
    if not value:
        return fallback
    # Athena may return 'YYYY-MM-DD 00:00:00.000' for date_trunc results.
    date_part = value[:10]
    try:
        year, month, day = date_part.split("-")
        return date(int(year), int(month), int(day))
    except Exception:
        return fallback


def _parse_rows(raw_rows: list[dict[str, str]], default_month: date) -> tuple[dict[str, RegionCostInput], date, list[str]]:
    parsed: dict[str, RegionCostInput] = {}
    effective_month = default_month
    unmapped: list[str] = []
    for row in raw_rows:
        region_raw = (row.get("region") or "").strip()
        region = _normalize_region(region_raw)
        if not region:
            continue
        if not re.match(r"^[a-z]{2}(-gov)?-[a-z]+-\d$", region):
            unmapped.append(region_raw or region)
        month_text = (row.get("month_start") or str(default_month)).strip()
        effective_month = _parse_month_start(month_text, fallback=effective_month)
        parsed[region] = RegionCostInput(
            month_start=month_text,
            region=region,
            ci_qty=_to_float(row.get("ci_qty")),
            ci_cost_usd=_to_float(row.get("ci_cost_usd")),
            eval_qty=_to_float(row.get("eval_qty")),
            eval_cost_usd=_to_float(row.get("eval_cost_usd")),
            total_config_cost_usd=_to_float(row.get("total_config_cost_usd")),
        )
    return parsed, effective_month, sorted(set([u for u in unmapped if u]))


def _run_athena_query(
    sess,
    sql: str,
    workgroup: str,
    output_s3: str,
    athena_region: str | None = None,
    poll_seconds: float = 1.0,
    timeout_seconds: int = 180,
) -> list[dict[str, str]]:
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


def get_last_full_month_config_costs_by_region(
    sess,
    account_id: str,
    database: str,
    table: str,
    workgroup: str,
    output_s3: str,
    athena_region: str | None = "us-east-1",
) -> CurCostResult:
    month_start = _last_full_month_start(date.today())
    month_days = monthrange(month_start.year, month_start.month)[1]
    table_ref = _table_ref(database=database, table=table)
    escaped_account = account_id.replace("'", "''")

    sql_last_full = _build_sql(
        table_ref=table_ref,
        escaped_account=escaped_account,
        month_selector_sql="(date_trunc('month', current_date) - INTERVAL '1' MONTH)",
    )
    raw_rows = _run_athena_query(
        sess,
        sql=sql_last_full,
        workgroup=workgroup,
        output_s3=output_s3,
        athena_region=athena_region,
    )
    parsed, effective_month, unmapped = _parse_rows(raw_rows, default_month=month_start)
    if parsed:
        effective_days = monthrange(effective_month.year, effective_month.month)[1]
        warning = None
        if unmapped:
            warning = f"CUR rows contained unmapped region labels: {', '.join(unmapped[:5])}"
        return CurCostResult(
            rows=parsed,
            sql=sql_last_full,
            month_start=effective_month,
            month_days=effective_days,
            fallback_used=False,
            warning=warning,
        )

    latest_month_selector = _latest_available_month_sql(table_ref=table_ref, escaped_account=escaped_account)
    sql_latest = _build_sql(
        table_ref=table_ref,
        escaped_account=escaped_account,
        month_selector_sql=latest_month_selector,
    )
    raw_rows_latest = _run_athena_query(
        sess,
        sql=sql_latest,
        workgroup=workgroup,
        output_s3=output_s3,
        athena_region=athena_region,
    )
    parsed_latest, effective_latest_month, unmapped_latest = _parse_rows(raw_rows_latest, default_month=month_start)
    if parsed_latest:
        effective_days = monthrange(effective_latest_month.year, effective_latest_month.month)[1]
        warning_parts = ["No rows in last full month; used latest available month with AWS Config usage."]
        if unmapped_latest:
            warning_parts.append(f"Unmapped region labels: {', '.join(unmapped_latest[:5])}")
        return CurCostResult(
            rows=parsed_latest,
            sql=sql_latest,
            month_start=effective_latest_month,
            month_days=effective_days,
            fallback_used=True,
            warning=" ".join(warning_parts),
        )

    return CurCostResult(
        rows={},
        sql=sql_last_full,
        month_start=month_start,
        month_days=month_days,
        fallback_used=False,
        warning="No AWS Config CUR rows found for this account/table in last full month or latest available month.",
    )
