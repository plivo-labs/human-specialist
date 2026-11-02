"""LenientModel request models: a JSON null on a plain string/bool field takes the zero value ("" / False)
instead of failing validation, because callers send null for unset fields (e.g. a transfer
destination or contact uuid read from a cache miss).
"""

import pytest

from hs.voice.transfer_models import (
    AgentDropParams,
    GetAgentsByCapacity,
    GetAgentsParams,
    IncomingTransferReq,
    OutboundQueueTransfer,
)

# (model, payload-with-null, field-name, expected-coerced-value)
CASES = [
    # transfer.py
    (GetAgentsParams, {"transfer_destination_id": None}, "transfer_destination_id", ""),
    (GetAgentsByCapacity, {"aom_uuid": None}, "aom_uuid", ""),
    (OutboundQueueTransfer, {"contact_uuid": None}, "contact_uuid", ""),
    (IncomingTransferReq, {"transfer_type": None, "identifier": None, "member": None}, "transfer_type", ""),
    (AgentDropParams, {"leave_type": None, "contact_uuid": None, "channel": None}, "leave_type", ""),
]


@pytest.mark.parametrize("model, payload, field, expected", CASES)
def test_null_is_coerced_not_rejected(model, payload, field, expected):
    # Must not raise ValidationError (the pre-fix behavior); null falls back to the field default.
    obj = model.model_validate(payload)
    assert getattr(obj, field) == expected


def test_non_null_values_still_bind():
    # Sanity: coercion only drops nulls; real values still populate.
    obj = GetAgentsParams.model_validate({"transfer_destination_id": "team-123", "auth_id": "AUTH"})
    assert obj.transfer_destination_id == "team-123"
    assert obj.auth_id == "AUTH"
