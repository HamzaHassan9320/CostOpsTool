from __future__ import annotations
import pandas as pd
from pathlib import Path
from app.core.types import Finding
from app.outputs.report_builder import findings_to_rows

def write_excel(findings: list[Finding], out_path: str) -> str:
    out = Path(out_path)
    df = pd.DataFrame(findings_to_rows(findings)).sort_values(
        by=["service", "severity", "optimization_id"], ascending=[True, False, True]
    )

    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        df.to_excel(xw, sheet_name="Findings", index=False)

    return str(out)