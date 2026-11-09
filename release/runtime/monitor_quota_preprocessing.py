"""Accept local quota observations before upload and canonical processing."""

import copy
from datetime import datetime, timezone

from monitor_canonical_store import CanonicalRepository
from monitor_common import parse_timestamp, resets_match
from monitor_sampling import WINDOWS, quota_point
from monitor_usage_sync import record_account_key


class QuotaPreprocessor:
    def __init__(self, index):
        self.index = index
        self.pending, self.observations = {}, {}

    def process(self, db, row):
        account = record_account_key(row)
        state = self.index.get(db, f"samplingState:local:{account}", {"windows": {}})
        point = quota_point(row)
        result = []
        for name, (label, _) in WINDOWS.items():
            if label not in row["windows"]:
                continue
            value = point[name]
            value.pop("continuous", None)
            value["accepted"] = False
            stamp = int(point["timestamp"])
            if value["raw"] is None or not 0 <= value["raw"] <= 100:
                continue
            previous = state["windows"].get(name)
            if previous is None:
                for prior in CanonicalRepository.rows(db, "local", "quota", account, condition="window=?", parameters=(label,), order="last DESC", limit=1):
                    window = prior["windows"][label]
                    previous = state["windows"][name] = {"raw": window["usedPercent"], "timestamp": window["lastAt"],
                        "checkedAt": datetime.fromtimestamp(window["lastAt"], timezone.utc).isoformat(), "resetAt": window.get("resetAt"), "plan": window["plan"]}
            key = account, label
            pending = self.pending.get(key)
            if previous and stamp < previous["timestamp"]:
                continue
            if pending and stamp <= parse_timestamp(pending["checkedAt"]):
                continue  # Confirmation requires a later local observation.
            current = row | {"windows": {label: row["windows"][label]}}
            if previous is None or not resets_match(value.get("resetAt"), previous.get("resetAt")):
                self.pending.pop(key, None)
                result.append(current)
            elif pending:
                if value["raw"] < pending["windows"][label]["usedPercent"]:
                    self.pending[key] = copy.deepcopy(current)
                    continue
                result.extend((self.pending.pop(key), current))
            elif value["raw"] < previous["raw"]:
                self.pending[key] = copy.deepcopy(current)
                continue
            else:
                result.append(current)
            value["accepted"] = True
            state["windows"][name] = {"raw": value["raw"], "timestamp": stamp, "checkedAt": row["checkedAt"],
                "resetAt": value.get("resetAt"), "plan": value["plan"]}
        state["timestamp"] = max((window["timestamp"] for window in state["windows"].values()), default=-1)
        self.index.put(db, f"samplingState:local:{account}", state)
        self.observations[account] = point
        return result
