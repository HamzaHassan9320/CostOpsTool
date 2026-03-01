from __future__ import annotations
from dataclasses import dataclass
from typing import Any

@dataclass
class ConfigRegionSnapshot:
    region: str
    recorder: dict | None
    recorder_status: dict | None
    delivery_channels: list[dict]
    rules_count: int
    rules_with_maxfreq: int

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
    rules = cfg.describe_config_rules().get("ConfigRules", [])
    rules_count = len(rules)
    rules_with_maxfreq = sum(1 for r in rules if r.get("MaximumExecutionFrequency"))

    return ConfigRegionSnapshot(
        region=region,
        recorder=recorder,
        recorder_status=recorder_status,
        delivery_channels=dcs,
        rules_count=rules_count,
        rules_with_maxfreq=rules_with_maxfreq,
    )