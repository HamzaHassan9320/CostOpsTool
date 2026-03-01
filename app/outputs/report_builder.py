from __future__ import annotations
from dataclasses import asdict
from app.core.types import Finding

def findings_to_rows(findings: list[Finding]) -> list[dict]:
    rows = []
    for f in findings:
        rows.append({
            "service": f.service,
            "optimization_id": f.optimization_id,
            "title": f.title,
            "account_id": f.account_id,
            "region": f.region,
            "severity": f.severity,
            "effort": f.effort,
            "risk": f.risk,
            "est_monthly_savings_usd": f.est_monthly_savings_usd,
            "recommendation": f.recommendation,
            "evidence": "; ".join([f"{e.key}={e.value}" for e in f.evidence]),
        })
    return rows