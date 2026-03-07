from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.core.types import NatRecommendationRow


NAT_ACTIVITY_PERIOD_SECONDS = 3600
SIX_MONTH_DAYS = 183
TWO_MONTH_DAYS = 61


@dataclass
class NatGatewayInfo:
    nat_gateway_id: str
    gateway_name: str
    region: str
    state: str
    connectivity_type: str
    vpc_id: str
    subnet_id: str
    public_ips: list[str]
    allocation_ids: list[str]


@dataclass
class NatGatewayActivity:
    bytes_out_to_destination_sum_6m: float
    bytes_out_to_source_sum_6m: float
    active_connection_max_6m: float
    bytes_out_to_destination_sum_2m: float
    bytes_out_to_source_sum_2m: float
    active_connection_max_2m: float
    datapoint_count_6m: int
    datapoint_count_2m: int


@dataclass
class IdleNatCandidate:
    gateway: NatGatewayInfo
    activity: NatGatewayActivity
    lookback_duration: str


@dataclass
class NatGatewayScanSummary:
    nat_gateway_count_scanned: int
    nat_gateway_idle_6m_count: int
    nat_gateway_idle_2m_count: int
    nat_metric_error_count: int
    nat_metric_error_samples: list[str]


def list_regions(sess) -> list[str]:
    ec2 = sess.client("ec2")
    return [r["RegionName"] for r in ec2.describe_regions(AllRegions=False)["Regions"]]


def _name_tag(tags: list[dict]) -> str:
    for tag in tags:
        if (tag.get("Key") or "").strip() == "Name":
            return (tag.get("Value") or "").strip()
    return ""


def list_nat_gateways(sess, regions: list[str]) -> tuple[list[NatGatewayInfo], list[str]]:
    gateways: list[NatGatewayInfo] = []
    errors: list[str] = []
    for region in regions:
        try:
            ec2 = sess.client("ec2", region_name=region)
            token = None
            while True:
                kwargs = {"MaxResults": 100}
                if token:
                    kwargs["NextToken"] = token
                resp = ec2.describe_nat_gateways(**kwargs)
                for nat in resp.get("NatGateways", []):
                    nat_id = (nat.get("NatGatewayId") or "").strip().lower()
                    if not nat_id:
                        continue
                    addresses = nat.get("NatGatewayAddresses") or []
                    public_ips = [a.get("PublicIp", "").strip().lower() for a in addresses if a.get("PublicIp")]
                    allocation_ids = [a.get("AllocationId", "").strip() for a in addresses if a.get("AllocationId")]
                    gateways.append(
                        NatGatewayInfo(
                            nat_gateway_id=nat_id,
                            gateway_name=_name_tag(nat.get("Tags") or []),
                            region=region,
                            state=(nat.get("State") or "").strip().lower(),
                            connectivity_type=(nat.get("ConnectivityType") or "public").strip().lower(),
                            vpc_id=(nat.get("VpcId") or "").strip(),
                            subnet_id=(nat.get("SubnetId") or "").strip(),
                            public_ips=public_ips,
                            allocation_ids=allocation_ids,
                        )
                    )
                token = resp.get("NextToken")
                if not token:
                    break
        except Exception as ex:
            errors.append(f"{region}:{ex}")
    return gateways, errors


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _sum_values(values: list[float]) -> float:
    return float(sum(values)) if values else 0.0


def _max_values(values: list[float]) -> float:
    return float(max(values)) if values else 0.0


def _collect_nat_metric_series(
    cloudwatch,
    nat_gateway_id: str,
    end_time: datetime,
) -> dict[str, tuple[list[datetime], list[float]]]:
    start_time = end_time - timedelta(days=SIX_MONTH_DAYS)
    queries = [
        {
            "Id": "bytesdest",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/NATGateway",
                    "MetricName": "BytesOutToDestination",
                    "Dimensions": [{"Name": "NatGatewayId", "Value": nat_gateway_id}],
                },
                "Period": NAT_ACTIVITY_PERIOD_SECONDS,
                "Stat": "Sum",
            },
            "ReturnData": True,
        },
        {
            "Id": "bytessource",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/NATGateway",
                    "MetricName": "BytesOutToSource",
                    "Dimensions": [{"Name": "NatGatewayId", "Value": nat_gateway_id}],
                },
                "Period": NAT_ACTIVITY_PERIOD_SECONDS,
                "Stat": "Sum",
            },
            "ReturnData": True,
        },
        {
            "Id": "activeconn",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/NATGateway",
                    "MetricName": "ActiveConnectionCount",
                    "Dimensions": [{"Name": "NatGatewayId", "Value": nat_gateway_id}],
                },
                "Period": NAT_ACTIVITY_PERIOD_SECONDS,
                "Stat": "Maximum",
            },
            "ReturnData": True,
        },
    ]

    aggregated: dict[str, tuple[list[datetime], list[float]]] = {
        "bytesdest": ([], []),
        "bytessource": ([], []),
        "activeconn": ([], []),
    }
    token = None
    while True:
        kwargs = {
            "MetricDataQueries": queries,
            "StartTime": start_time,
            "EndTime": end_time,
            "ScanBy": "TimestampAscending",
        }
        if token:
            kwargs["NextToken"] = token
        resp = cloudwatch.get_metric_data(**kwargs)
        for result in resp.get("MetricDataResults", []):
            result_id = (result.get("Id") or "").strip()
            if result_id not in aggregated:
                continue
            timestamps = [_as_utc(ts) for ts in (result.get("Timestamps") or [])]
            values = [float(v) for v in (result.get("Values") or [])]
            aggregated[result_id][0].extend(timestamps)
            aggregated[result_id][1].extend(values)
        token = resp.get("NextToken")
        if not token:
            break
    return aggregated


def collect_nat_gateway_activity(
    sess,
    gateways: list[NatGatewayInfo],
    now: datetime | None = None,
) -> tuple[dict[str, NatGatewayActivity], list[str]]:
    if now is None:
        now = datetime.now(timezone.utc)
    now = _as_utc(now)
    two_month_start = now - timedelta(days=TWO_MONTH_DAYS)

    clients: dict[str, object] = {}
    activity_by_id: dict[str, NatGatewayActivity] = {}
    errors: list[str] = []

    for gateway in gateways:
        if gateway.state != "available":
            continue
        try:
            cloudwatch = clients.get(gateway.region)
            if cloudwatch is None:
                cloudwatch = sess.client("cloudwatch", region_name=gateway.region)
                clients[gateway.region] = cloudwatch
            series = _collect_nat_metric_series(cloudwatch=cloudwatch, nat_gateway_id=gateway.nat_gateway_id, end_time=now)
            bytesdest_ts, bytesdest_vals = series["bytesdest"]
            bytessource_ts, bytessource_vals = series["bytessource"]
            active_ts, active_vals = series["activeconn"]

            bytesdest_vals_2m = [v for ts, v in zip(bytesdest_ts, bytesdest_vals) if ts >= two_month_start]
            bytessource_vals_2m = [v for ts, v in zip(bytessource_ts, bytessource_vals) if ts >= two_month_start]
            active_vals_2m = [v for ts, v in zip(active_ts, active_vals) if ts >= two_month_start]

            activity_by_id[gateway.nat_gateway_id] = NatGatewayActivity(
                bytes_out_to_destination_sum_6m=_sum_values(bytesdest_vals),
                bytes_out_to_source_sum_6m=_sum_values(bytessource_vals),
                active_connection_max_6m=_max_values(active_vals),
                bytes_out_to_destination_sum_2m=_sum_values(bytesdest_vals_2m),
                bytes_out_to_source_sum_2m=_sum_values(bytessource_vals_2m),
                active_connection_max_2m=_max_values(active_vals_2m),
                datapoint_count_6m=max(len(bytesdest_vals), len(bytessource_vals), len(active_vals)),
                datapoint_count_2m=max(len(bytesdest_vals_2m), len(bytessource_vals_2m), len(active_vals_2m)),
            )
        except Exception as ex:
            errors.append(f"{gateway.region}:{gateway.nat_gateway_id}:{ex}")

    return activity_by_id, errors


def _is_zero(value: float) -> bool:
    return abs(float(value)) <= 0.0


def identify_idle_nat_gateways(
    gateways: list[NatGatewayInfo],
    activity_by_nat_id: dict[str, NatGatewayActivity],
    activity_errors: list[str] | None = None,
) -> tuple[list[IdleNatCandidate], NatGatewayScanSummary]:
    idle_6m_count = 0
    idle_2m_count = 0
    candidates: list[IdleNatCandidate] = []

    available_gateway_count = sum(1 for g in gateways if g.state == "available")
    for gateway in gateways:
        if gateway.state != "available":
            continue
        activity = activity_by_nat_id.get(gateway.nat_gateway_id)
        if activity is None:
            continue

        idle_6m = (
            _is_zero(activity.bytes_out_to_destination_sum_6m)
            and _is_zero(activity.bytes_out_to_source_sum_6m)
            and _is_zero(activity.active_connection_max_6m)
        )
        idle_2m = (
            _is_zero(activity.bytes_out_to_destination_sum_2m)
            and _is_zero(activity.bytes_out_to_source_sum_2m)
            and _is_zero(activity.active_connection_max_2m)
        )

        if idle_6m:
            idle_6m_count += 1
            candidates.append(IdleNatCandidate(gateway=gateway, activity=activity, lookback_duration="6 months"))
        elif idle_2m:
            idle_2m_count += 1
            candidates.append(IdleNatCandidate(gateway=gateway, activity=activity, lookback_duration="2 months"))

    summary = NatGatewayScanSummary(
        nat_gateway_count_scanned=available_gateway_count,
        nat_gateway_idle_6m_count=idle_6m_count,
        nat_gateway_idle_2m_count=idle_2m_count,
        nat_metric_error_count=len(activity_errors or []),
        nat_metric_error_samples=(activity_errors or [])[:5],
    )
    return candidates, summary


def build_nat_recommendations(
    account_id: str | None,
    candidates: list[IdleNatCandidate],
    monthly_cost_by_nat_id: dict[str, float],
    eip_monthly_price_per_eip: float | None = None,
) -> list[NatRecommendationRow]:
    def _attached_address_count(candidate: IdleNatCandidate) -> int:
        return max(len(candidate.gateway.public_ips), len(candidate.gateway.allocation_ids))

    def _total_monthly_cost(candidate: IdleNatCandidate) -> float | None:
        nat_component = monthly_cost_by_nat_id.get(candidate.gateway.nat_gateway_id)
        eip_component = None
        if eip_monthly_price_per_eip is not None:
            eip_component = float(_attached_address_count(candidate)) * float(eip_monthly_price_per_eip)

        if nat_component is None and eip_component is None:
            return None
        return float(nat_component or 0.0) + float(eip_component or 0.0)

    rows: list[NatRecommendationRow] = []
    for candidate in candidates:
        if candidate.lookback_duration == "6 months":
            bytes_dest = candidate.activity.bytes_out_to_destination_sum_6m
            bytes_source = candidate.activity.bytes_out_to_source_sum_6m
            active = candidate.activity.active_connection_max_6m
        else:
            bytes_dest = candidate.activity.bytes_out_to_destination_sum_2m
            bytes_source = candidate.activity.bytes_out_to_source_sum_2m
            active = candidate.activity.active_connection_max_2m

        rows.append(
            NatRecommendationRow(
                account_id=account_id,
                region=candidate.gateway.region,
                gateway_name=candidate.gateway.gateway_name,
                gateway_id=candidate.gateway.nat_gateway_id,
                lookback_duration=candidate.lookback_duration,
                bytes_out_to_destination=bytes_dest,
                bytes_out_to_source=bytes_source,
                active_connections=active,
                monthly_cost=_total_monthly_cost(candidate),
            )
        )
    return rows
