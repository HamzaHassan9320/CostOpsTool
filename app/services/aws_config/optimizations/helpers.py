from __future__ import annotations


def apply_edp(gross_monthly: float | None, edp_percent: float) -> tuple[float | None, float | None, float | None, float | None, float | None]:
    if gross_monthly is None:
        return None, None, None, None, None
    gross_monthly = float(gross_monthly)
    net_monthly = gross_monthly * (1.0 - (edp_percent / 100.0))
    gross_annual = gross_monthly * 12.0
    net_annual = net_monthly * 12.0
    # est_monthly_savings_usd is retained for backward compatibility and maps to gross monthly.
    return gross_monthly, gross_monthly, net_monthly, gross_annual, net_annual
