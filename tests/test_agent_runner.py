from app.agent import nat_agent as mod
from app.core.types import RunContext


def test_recovered_llm_tool_error_is_diagnostic_not_warning(monkeypatch):
    monkeypatch.setattr(mod, 'make_boto3_session', lambda profile_name: object())
    monkeypatch.setattr(
        mod,
        '_run_with_llm_tools',
        lambda toolset: (True, 'LLM tool-calling failed: Athena query timed out while waiting for completion.'),
    )

    def fake_fallback(self):
        self.diagnostics['recommendations'] = {'recommendation_count': 0}

    monkeypatch.setattr(mod.NatOptimizationToolset, 'run_remaining_deterministic', fake_fallback)

    result = mod.run_nat_optimization_agent(
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

    assert result.warnings == []
    assert 'llm_tool_error_recovered' in result.diagnostics
