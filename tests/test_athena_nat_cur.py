from app.services.nat.costs import athena_nat_cur as mod


def test_build_sql_contains_nat_id_scope_and_net_amortized_case():
    sql = mod._build_sql(
        table_ref='"cost"."cudos01"',
        escaped_account='123456789012',
        nat_gateway_ids=['nat-0783ffd63f3ea022f', 'nat-09fea4af6989717f9'],
    )
    assert "regexp_extract(lower(coalesce(line_item_resource_id,'')), 'nat-[0-9a-f]+')" in sql
    assert "line_item_line_item_type <> 'Tax'" in sql
    assert "LIKE '%natgateway-hours%'" in sql
    assert "nat_gateway_id IN" in sql
    assert "'nat-0783ffd63f3ea022f'" in sql
    assert "'nat-09fea4af6989717f9'" in sql
    assert "SavingsPlanRecurringFee" in sql
    assert "reservation_net_effective_cost" in sql


def test_nat_id_validation_filters_invalid_values():
    safe_ids = mod._validate_nat_ids(['nat-abc123', 'nat-ABC123', 'nat-zzz', 'drop table', '', 'nat-123'])
    assert safe_ids == ['nat-123', 'nat-abc123']


def test_nat_cur_no_rows_returns_warning(monkeypatch):
    monkeypatch.setattr(mod, '_run_athena_query', lambda *args, **kwargs: [])
    result = mod.get_last_full_month_nat_gateway_net_amortized_costs_by_ids(
        sess=object(),
        account_id='123456789012',
        database='cost',
        table='cudos01',
        workgroup='primary',
        output_s3='s3://bucket/prefix',
        nat_gateway_ids=['nat-0783ffd63f3ea022f'],
        athena_region='us-east-1',
    )
    assert result.monthly_cost_by_nat_id == {}
    assert result.lines == []
    assert result.warning is not None
    assert 'last full month' in result.warning


def test_nat_cur_parses_grouped_rows(monkeypatch):
    nat_rows = [
        {
            'nat_gateway_id': 'nat-0783ffd63f3ea022f',
            'product_region': 'eu-west-1',
            'line_item_usage_type': 'EU-NatGateway-Hours',
            'line_item_operation': 'NatGateway',
            'net_amortized_usd': '10.5',
        },
        {
            'nat_gateway_id': 'nat-0783ffd63f3ea022f',
            'product_region': 'eu-west-1',
            'line_item_usage_type': 'EU-NatGateway-Hours',
            'line_item_operation': 'NatGateway',
            'net_amortized_usd': '2.5',
        },
    ]

    def fake_run_query(*args, **kwargs):
        sql = kwargs.get('sql', '')
        if 'WITH eip_rate AS' in sql:
            return [
                {
                    'total_hours': '720',
                    'total_net_usd': '18',
                    'effective_usd_per_hour': '0.025',
                    'effective_monthly_price_per_eip': '18',
                }
            ]
        return nat_rows

    monkeypatch.setattr(mod, '_run_athena_query', fake_run_query)
    result = mod.get_last_full_month_nat_gateway_net_amortized_costs_by_ids(
        sess=object(),
        account_id='123456789012',
        database='cost',
        table='cudos01',
        workgroup='primary',
        output_s3='s3://bucket/prefix',
        nat_gateway_ids=['nat-0783ffd63f3ea022f'],
        athena_region='us-east-1',
    )
    assert result.warning is None
    assert result.monthly_cost_by_nat_id['nat-0783ffd63f3ea022f'] == 13.0
    assert result.eip_monthly_price_per_eip == 18.0
    assert len(result.lines) == 2
