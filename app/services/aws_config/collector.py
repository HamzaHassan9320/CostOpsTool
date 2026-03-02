from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json

@dataclass
class ConfigRegionSnapshot:
    region: str
    recorder: dict | None
    recorder_status: dict | None
    delivery_channels: list[dict]
    rules_count: int
    rules_with_maxfreq: int
    conformance_packs_count: int
    aggregators_count: int
    discovered_total_resources: int
    discovered_config_resources: int
    discovered_resources_excluding_config: int
    ci_30d_total: int
    ci_30d_iam: int
    ci_30d_resource_compliance: int


def _count_items_with_pagination(fetch_page) -> int:
    count = 0
    token = None
    while True:
        page = fetch_page(token)
        count += len(page.items)
        token = page.next_token
        if not token:
            break
    return count


class _PagedResult:
    def __init__(self, items: list[dict], next_token: str | None):
        self.items = items
        self.next_token = next_token


def _describe_rules_page(cfg, next_token: str | None) -> _PagedResult:
    kwargs = {"NextToken": next_token} if next_token else {}
    resp = cfg.describe_config_rules(**kwargs)
    return _PagedResult(resp.get("ConfigRules", []), resp.get("NextToken"))


def _describe_conformance_packs_page(cfg, next_token: str | None) -> _PagedResult:
    kwargs = {"NextToken": next_token} if next_token else {}
    resp = cfg.describe_conformance_packs(**kwargs)
    return _PagedResult(resp.get("ConformancePackDetails", []), resp.get("NextToken"))


def _describe_aggregators_page(cfg, next_token: str | None) -> _PagedResult:
    kwargs = {"NextToken": next_token} if next_token else {}
    resp = cfg.describe_configuration_aggregators(**kwargs)
    return _PagedResult(resp.get("ConfigurationAggregators", []), resp.get("NextToken"))


def _parse_count_result(raw: str | None) -> int:
    if not raw:
        return 0
    obj = json.loads(raw)
    if not obj:
        return 0
    value = list(obj.values())[0]
    return int(float(value))


def _select_count(cfg, expression: str) -> int:
    resp = cfg.select_resource_config(Expression=expression, Limit=1)
    rows = resp.get("Results", [])
    if not rows:
        return 0
    return _parse_count_result(rows[0])


def _iso_utc_30d_ago() -> str:
    t = datetime.now(timezone.utc) - timedelta(days=30)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _operation_token_name(cfg, operation_name: str) -> str:
    model = cfg.meta.service_model.operation_model(operation_name)
    members = set((model.input_shape.members or {}).keys()) if model.input_shape else set()
    if "nextToken" in members:
        return "nextToken"
    if "NextToken" in members:
        return "NextToken"
    return "nextToken"


def _discovered_resource_counts(cfg) -> tuple[int, int]:
    total = 0
    config_only = 0
    resp = cfg.get_discovered_resource_counts()
    total = max(total, int(resp.get("totalDiscoveredResources", 0)))
    for rc in resp.get("resourceCounts", []):
        if (rc.get("resourceType") or "").startswith("AWS::Config::"):
            config_only += int(rc.get("count", 0))

    next_token = resp.get("nextToken") or resp.get("NextToken")
    token_candidates = [_operation_token_name(cfg, "GetDiscoveredResourceCounts"), "nextToken", "NextToken"]
    seen_tokens: set[str] = set()
    while next_token and next_token not in seen_tokens:
        seen_tokens.add(next_token)
        next_resp = None
        last_validation_error = None
        for token_arg_name in token_candidates:
            try:
                next_resp = cfg.get_discovered_resource_counts(**{token_arg_name: next_token})
                break
            except Exception as ex:
                msg = str(ex)
                if "Unknown parameter in input" in msg and "nextToken" in msg:
                    last_validation_error = ex
                    continue
                raise
        if next_resp is None:
            # Stop pagination if token parameter casing mismatches in this runtime.
            if last_validation_error:
                break
            break

        total = max(total, int(next_resp.get("totalDiscoveredResources", 0)))
        for rc in next_resp.get("resourceCounts", []):
            if (rc.get("resourceType") or "").startswith("AWS::Config::"):
                config_only += int(rc.get("count", 0))
        next_token = next_resp.get("nextToken") or next_resp.get("NextToken")
    return total, config_only

def collect_config_region(sess, region: str) -> ConfigRegionSnapshot:
    cfg = sess.client("config", region_name=region)

    # recorder
    recs = cfg.describe_configuration_recorders().get("ConfigurationRecorders", [])
    recorder = recs[0] if recs else None

    # status
    statuses = cfg.describe_configuration_recorder_status().get("ConfigurationRecordersStatus", [])
    recorder_status = statuses[0] if statuses else None

    # delivery channels
    dcs = cfg.describe_delivery_channels().get("DeliveryChannels", [])

    # rules
    rules = []
    next_token = None
    while True:
        page = _describe_rules_page(cfg, next_token)
        rules.extend(page.items)
        next_token = page.next_token
        if not next_token:
            break
    rules_count = len(rules)
    rules_with_maxfreq = sum(1 for r in rules if r.get("MaximumExecutionFrequency"))

    # dependencies
    conformance_packs_count = _count_items_with_pagination(
        lambda token: _describe_conformance_packs_page(cfg, token)
    )
    aggregators_count = _count_items_with_pagination(lambda token: _describe_aggregators_page(cfg, token))

    # inventory
    discovered_total_resources, discovered_config_resources = _discovered_resource_counts(cfg)
    discovered_resources_excluding_config = max(0, discovered_total_resources - discovered_config_resources)

    # 30d counts using advanced query
    since_iso = _iso_utc_30d_ago()
    ci_30d_total = _select_count(
        cfg,
        (
            "SELECT COUNT(*) "
            f"WHERE configurationItemCaptureTime >= '{since_iso}'"
        ),
    )
    ci_30d_iam = _select_count(
        cfg,
        (
            "SELECT COUNT(*) "
            f"WHERE configurationItemCaptureTime >= '{since_iso}' "
            "AND resourceType LIKE 'AWS::IAM::%'"
        ),
    )
    ci_30d_resource_compliance = _select_count(
        cfg,
        (
            "SELECT COUNT(*) "
            f"WHERE configurationItemCaptureTime >= '{since_iso}' "
            "AND resourceType = 'AWS::Config::ResourceCompliance'"
        ),
    )

    return ConfigRegionSnapshot(
        region=region,
        recorder=recorder,
        recorder_status=recorder_status,
        delivery_channels=dcs,
        rules_count=rules_count,
        rules_with_maxfreq=rules_with_maxfreq,
        conformance_packs_count=conformance_packs_count,
        aggregators_count=aggregators_count,
        discovered_total_resources=discovered_total_resources,
        discovered_config_resources=discovered_config_resources,
        discovered_resources_excluding_config=discovered_resources_excluding_config,
        ci_30d_total=ci_30d_total,
        ci_30d_iam=ci_30d_iam,
        ci_30d_resource_compliance=ci_30d_resource_compliance,
    )
