from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.core.types import NatCurCostLine, NatRecommendationRow

RECOMMENDATION_COLUMNS = [
    "Account ID",
    "Region",
    "Gateway Name",
    "Gateway ID",
    "Lookback Duration",
    "BytesOutToDestination",
    "BytesOutToSource",
    "Active Connections",
    "Monthly Cost",
]

def _recommendations_df(recommendations: list[NatRecommendationRow]) -> pd.DataFrame:
    rows = []
    for row in recommendations:
        rows.append(
            {
                "Account ID": row.account_id,
                "Region": row.region,
                "Gateway Name": row.gateway_name,
                "Gateway ID": row.gateway_id,
                "Lookback Duration": row.lookback_duration,
                "BytesOutToDestination": row.bytes_out_to_destination,
                "BytesOutToSource": row.bytes_out_to_source,
                "Active Connections": row.active_connections,
                "Monthly Cost": row.monthly_cost,
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=RECOMMENDATION_COLUMNS)
    return df[RECOMMENDATION_COLUMNS]


def write_excel(
    recommendations: list[NatRecommendationRow],
    out_path: str,
    account_id: str | None = None,
    sql_used: str | None = None,
    cur_cost_lines: list[NatCurCostLine] | None = None,
    warnings: list[str] | None = None,
    diagnostics: dict | None = None,
) -> str:
    out = Path(out_path)
    recommendations_df = _recommendations_df(recommendations)

    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        recommendations_df.to_excel(xw, sheet_name="Recommendations", index=False)

    return str(out)
