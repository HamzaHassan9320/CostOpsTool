from pydantic import BaseModel, Field
from typing import Literal

class RouterOutput(BaseModel):
    action: str = Field(..., description="Action id, e.g. aws_config.savings_scan")
    profile_name: str = Field(..., description="Local AWS profile name (SSO-based)")
    days: int = Field(30, ge=1, le=365)
    output: Literal["excel", "json", "markdown"] = "excel"