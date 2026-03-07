from app.core.types import AgentRunResult, RunContext
from app.services.nat import plugin as mod


def test_plugin_calls_agent(monkeypatch):
    expected = AgentRunResult(
        recommendations=[],
        diagnostics={'ok': True},
        sql_used='select 1',
        cur_cost_lines=[],
        warnings=[],
    )

    monkeypatch.setattr(mod, 'run_nat_optimization_agent', lambda ctx: expected)

    result = mod.NatOptimizationScan().run(
        RunContext(
            profile_name='demo',
            account_id='123456789012',
            days=30,
            regions=[],
            athena_database='cost',
            athena_table='cudos01',
            athena_workgroup='primary',
            athena_output_s3='s3://bucket/prefix',
            athena_profile_name='demo',
            athena_region='us-east-1',
            requested_by=None,
        )
    )

    assert result.diagnostics['ok'] is True
    assert result.sql_used == 'select 1'
