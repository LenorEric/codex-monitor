"""Current cloud format gate and initialization of empty synchronization roots."""

import json

CLOUD_DATA_CONTRACT_VERSION = 5
CLOUD_DATA_CONTRACT_PATH = "data-contract.json"


def read_cloud_contract(client):
    from monitor_cloud import CloudError
    try:
        data, etag = client.get(CLOUD_DATA_CONTRACT_PATH)
    except CloudError as exc:
        if exc.http_status == 404:
            return 0, None
        raise
    try:
        value = json.loads(data)
    except (ValueError, UnicodeDecodeError) as exc:
        raise CloudError("Invalid cloud data contract metadata; cloud access is blocked", 409) from exc
    version = value.get("dataContractVersion") if isinstance(value, dict) else None
    if type(version) is not int or version < 0:
        raise CloudError("Invalid cloud data contract version; cloud access is blocked", 409)
    if version > CLOUD_DATA_CONTRACT_VERSION:
        raise CloudError(f"Cloud data contract {version} is newer than this app's target {CLOUD_DATA_CONTRACT_VERSION}; cloud access is blocked until the local script is updated", 409, category="update_required")
    if version != CLOUD_DATA_CONTRACT_VERSION:
        raise CloudError(f"Cloud data contract {version} is unsupported; use cloud data already converted to contract {CLOUD_DATA_CONTRACT_VERSION}. Automatic cloud migration has been removed", 409)
    return version, etag


def require_cloud_contract(client):
    from monitor_cloud import CloudError
    from monitor_cloud_lock import CLOUD_LOCK_PATH, CLOUD_LOCK_INIT_PATH
    current, _ = read_cloud_contract(client)
    if current == CLOUD_DATA_CONTRACT_VERSION:
        return
    try:
        names = set(client.list(""))
    except CloudError as exc:
        if exc.http_status != 404:
            raise
        names = set()
    if names - {CLOUD_LOCK_PATH, CLOUD_LOCK_INIT_PATH}:
        raise CloudError("Cloud data has no contract marker; automatic cloud migration has been removed. Existing files were left unchanged", 409)
    client.put(CLOUD_DATA_CONTRACT_PATH, json.dumps({"dataContractVersion": CLOUD_DATA_CONTRACT_VERSION}, separators=(",", ":")).encode(), create=True)
