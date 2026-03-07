from app.services.nat.optimization import (
    NatGatewayActivity,
    NatGatewayInfo,
    build_nat_recommendations,
    identify_idle_nat_gateways,
)


def _gateway(nat_id: str, name: str, region: str = 'eu-west-1') -> NatGatewayInfo:
    return NatGatewayInfo(
        nat_gateway_id=nat_id,
        gateway_name=name,
        region=region,
        state='available',
        connectivity_type='public',
        vpc_id='vpc-1',
        subnet_id='subnet-1',
        public_ips=['198.51.100.10'],
        allocation_ids=['eipalloc-1'],
    )


def test_identify_idle_nat_gateways_marks_6m_and_2m():
    gateways = [_gateway('nat-aaa111', 'gw-6m'), _gateway('nat-bbb222', 'gw-2m')]
    activity = {
        'nat-aaa111': NatGatewayActivity(
            bytes_out_to_destination_sum_6m=0.0,
            bytes_out_to_source_sum_6m=0.0,
            active_connection_max_6m=0.0,
            bytes_out_to_destination_sum_2m=0.0,
            bytes_out_to_source_sum_2m=0.0,
            active_connection_max_2m=0.0,
            datapoint_count_6m=100,
            datapoint_count_2m=30,
        ),
        'nat-bbb222': NatGatewayActivity(
            bytes_out_to_destination_sum_6m=11.0,
            bytes_out_to_source_sum_6m=7.0,
            active_connection_max_6m=3.0,
            bytes_out_to_destination_sum_2m=0.0,
            bytes_out_to_source_sum_2m=0.0,
            active_connection_max_2m=0.0,
            datapoint_count_6m=100,
            datapoint_count_2m=30,
        ),
    }

    candidates, summary = identify_idle_nat_gateways(gateways=gateways, activity_by_nat_id=activity, activity_errors=[])
    assert len(candidates) == 2
    by_id = {c.gateway.nat_gateway_id: c for c in candidates}
    assert by_id['nat-aaa111'].lookback_duration == '6 months'
    assert by_id['nat-bbb222'].lookback_duration == '2 months'
    assert summary.nat_gateway_idle_6m_count == 1
    assert summary.nat_gateway_idle_2m_count == 1


def test_build_nat_recommendations_uses_gateway_name_and_monthly_cost():
    gateways = [_gateway('nat-abc123', 'prod-egress')]
    activity = {
        'nat-abc123': NatGatewayActivity(
            bytes_out_to_destination_sum_6m=0.0,
            bytes_out_to_source_sum_6m=0.0,
            active_connection_max_6m=0.0,
            bytes_out_to_destination_sum_2m=0.0,
            bytes_out_to_source_sum_2m=0.0,
            active_connection_max_2m=0.0,
            datapoint_count_6m=100,
            datapoint_count_2m=30,
        )
    }

    candidates, _ = identify_idle_nat_gateways(gateways=gateways, activity_by_nat_id=activity, activity_errors=[])
    rows = build_nat_recommendations(
        account_id='123456789012',
        candidates=candidates,
        monthly_cost_by_nat_id={'nat-abc123': 42.5},
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.gateway_name == 'prod-egress'
    assert row.gateway_id == 'nat-abc123'
    assert row.lookback_duration == '6 months'
    assert row.monthly_cost == 42.5


def test_build_nat_recommendations_adds_eip_price_to_nat_monthly_cost():
    gateway = NatGatewayInfo(
        nat_gateway_id='nat-xyz789',
        gateway_name='prod-egress-2',
        region='eu-west-1',
        state='available',
        connectivity_type='public',
        vpc_id='vpc-2',
        subnet_id='subnet-2',
        public_ips=['198.51.100.20', '198.51.100.21'],
        allocation_ids=['eipalloc-a', 'eipalloc-b'],
    )
    activity = {
        'nat-xyz789': NatGatewayActivity(
            bytes_out_to_destination_sum_6m=0.0,
            bytes_out_to_source_sum_6m=0.0,
            active_connection_max_6m=0.0,
            bytes_out_to_destination_sum_2m=0.0,
            bytes_out_to_source_sum_2m=0.0,
            active_connection_max_2m=0.0,
            datapoint_count_6m=100,
            datapoint_count_2m=30,
        )
    }

    candidates, _ = identify_idle_nat_gateways(gateways=[gateway], activity_by_nat_id=activity, activity_errors=[])
    rows = build_nat_recommendations(
        account_id='123456789012',
        candidates=candidates,
        monthly_cost_by_nat_id={'nat-xyz789': 30.0},
        eip_monthly_price_per_eip=2.5,
    )

    assert len(rows) == 1
    assert rows[0].monthly_cost == 35.0
