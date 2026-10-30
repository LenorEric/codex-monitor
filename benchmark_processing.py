"""Reproducible idle/append scaling check; fixtures are removed after each workload."""

import argparse
import json
import shutil
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from monitor_projection import DashboardProjector
from monitor_usage_sync import UsageDataStore


def benchmark(count):
    root = Path.cwd() / f".processing-benchmark-{uuid.uuid4().hex}"
    root.mkdir()
    try:
        store = UsageDataStore(root / "quota.jsonl", root / "ledger.jsonl", "machine", lambda _: "v3:account", threading.RLock())
        def sample(index):
            return {"checkedAt": datetime.fromtimestamp(1893456000 + index, timezone.utc).isoformat(), "accountSlotId": "slot", "accountLabel": "Account",
                "windows": {"5h": {"usedPercent": 0, "plan": "plus"}, "7d": {"usedPercent": 0, "plan": "plus"}}}
        with store.index.transaction() as db:
            store.index.write_records(db, "quota", [store.normalize_new_quota(sample(index)) for index in range(count)], backup_path=store.quota_path)
        store.backup.flush()
        accounts = {"activeAccountId": "slot", "items": [{"id": "slot", "label": "Account", "usageAccountId": "v3:account"}]}
        projector = DashboardProjector(store)
        started = time.perf_counter()
        projector.refresh(accounts, 1893456000 + count)
        initial_seconds = time.perf_counter() - started
        before = dict(store.index.metrics)
        replayed = projector.metrics["quotaRowsReplayed"]
        started, cpu = time.perf_counter(), time.process_time()
        for _ in range(3):
            assert projector.refresh(accounts, 1893456000 + count) is None
        idle_seconds, idle_cpu = (time.perf_counter() - started) / 3, (time.process_time() - cpu) / 3
        assert store.index.metrics == before
        started, cpu = time.perf_counter(), time.process_time()
        store.record_quota(sample(count))
        projector.refresh(accounts, 1893456000 + count + 1)
        result = {"historicalRows": count, "initialSeconds": round(initial_seconds, 6), "idleSeconds": round(idle_seconds, 6), "idleCpuSeconds": round(idle_cpu, 6),
            "appendSeconds": round(time.perf_counter() - started, 6), "appendCpuSeconds": round(time.process_time() - cpu, 6),
            "appendCanonicalBytesRead": store.index.metrics["canonicalBytesRead"] - before["canonicalBytesRead"],
            "appendVerificationBytesRead": store.index.metrics["sourceVerificationBytes"] - before["sourceVerificationBytes"],
            "appendRecordsImported": store.index.metrics["recordsImported"] - before["recordsImported"], "appendQuotaRowsReplayed": projector.metrics["quotaRowsReplayed"] - replayed}
        assert result["appendRecordsImported"] == 1 and result["appendQuotaRowsReplayed"] == 0 and result["appendCanonicalBytesRead"] == 0, result
        return result
    finally:
        if not root.resolve().is_relative_to(Path.cwd().resolve()) or not root.name.startswith(".processing-benchmark-"):
            raise ValueError("Unexpected benchmark directory")
        shutil.rmtree(root)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", nargs="+", type=int, default=[1000, 10000, 100000])
    for count in parser.parse_args().rows:
        print(json.dumps(benchmark(count)), flush=True)
