from __future__ import annotations

from app.agent.nat_agent import run_nat_optimization_agent
from app.core.registry import register
from app.core.types import AgentRunResult, RunContext

ACTION_ID = "optimization.run_scan"


@register(ACTION_ID)
class NatOptimizationScan:
    id = ACTION_ID

    def run(self, ctx: RunContext) -> AgentRunResult:
        return run_nat_optimization_agent(ctx)
