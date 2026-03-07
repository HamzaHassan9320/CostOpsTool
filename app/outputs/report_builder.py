from __future__ import annotations

from app.core.types import NatRecommendationRow


def recommendations_to_rows(recommendations: list[NatRecommendationRow]) -> list[dict]:
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
    return rows
