"""Frozen two-file account reader for released cloud migrations and offline conversion."""

import base64
import hashlib
import json
from monitor_accounts import auth_identity, parse_auth_bytes
from monitor_cloud import CloudError

def download_legacy_account(client, box, state):
    key, revision = state.get("accountKey"), state.get("revisionId")
    if not key or not revision:
        raise CloudError("Remote account state is incomplete", 409)
    payload, _ = client.get(f"accounts/revisions/{key}/{revision}.enc")
    try:
        revision_data = json.loads(box.decrypt(f"account-revision:{key}:{revision}", payload))
        auth_data = base64.b64decode(revision_data["auth"], validate=True)
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        raise CloudError("Invalid remote account revision", 409) from exc
    identity = auth_identity(parse_auth_bytes(auth_data))
    account_id = state.get("accountId") or identity.get("accountId")
    state_account_id = state.get("accountId")
    state_email = state.get("email")
    if (
        revision_data.get("version") != 1 or revision_data.get("accountKey") != key or len(auth_data) != revision_data.get("authSize") or hashlib.sha256(auth_data).hexdigest() != revision_data.get("authSha256")
        or (hashlib.sha256(account_id.encode()).hexdigest() if account_id else None) != revision_data.get("accountIdHash")
        or (state_account_id and identity.get("accountId") != state_account_id)
        or (state_email and identity.get("email") != state_email)
    ):
        raise CloudError("Account revision completion check failed", 409)
    return auth_data

