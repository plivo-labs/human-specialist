"""The conversation status machinery (offline decision-tree pins).

* the pre-change status check: the ALLOWED_STATUS_CHANGES matrix + the no-integration skip + the
  waiting corner gates (call channel / non-open);
* update_conversation: guards (400 no-data, 404 no-conv, 422 parent-update-on-wrapup with
  error_code 1.001, 422 invalid transition with correct_status), the per-status child update
  shapes (custom-state + wrap_up_start_at / end_at / *_creation_start_at), Post invoked per
  primary'd conv with active_convs, last_activity bump;
* Post(waiting): the WC write (`WC:<channel>:<contact>` shape, chat-only);
* check_waiting_conversation_inprocess: WC hit → delete + PATCH {status: open} AS the waiting
  agent; miss → no-op;
* check_and_update_waiting_conv: a waiting child → the waiting→open row flip;
* the capacity-clearance fallback: schedules the worker payload (release=True, closed=False,
  continue=False) on the background-tasks context with the 5s countdown semantics;
* task helpers: generic/callback auto-resolve, other unresolved tasks block;
* the PATCH route binds the conversation update model and envelopes (errors.global_error +
  global_error_data).
"""

import uuid as uuid_mod
from types import SimpleNamespace

import pytest

import tests.conftest  # noqa: F401

AOM_UUID = str(uuid_mod.uuid4())
ORG_UUID = str(uuid_mod.uuid4())


@pytest.fixture(autouse=True)
def _ctx():
    from hs.infra.context import (
        context_aom_uuid,
        context_background_tasks,
        context_org_name,
        context_org_uuid,
        context_req_meta,
    )

    t = [
        context_req_meta.set({"api_id": "status-machinery-test"}),
        context_aom_uuid.set(AOM_UUID),
        context_org_uuid.set(ORG_UUID),
        context_org_name.set("plivo"),
    ]
    bg = []
    t.append(context_background_tasks.set(SimpleNamespace(add_task=lambda fn, *a, **k: bg.append((fn, a, k)))))
    yield bg
    from hs.infra.context import (
        context_aom_uuid,
        context_background_tasks,
        context_org_name,
        context_org_uuid,
        context_req_meta,
    )

    for var, tok in zip(
        (context_req_meta, context_aom_uuid, context_org_uuid, context_org_name, context_background_tasks), t
    ):
        var.reset(tok)


class _Post:
    calls = []

    def __init__(self, **kw):
        self.kw = kw

    def invoke(self):
        _Post.calls.append(self.kw)


def _parent(cn="CN1", crm=None, case=None, channel="chat"):
    return SimpleNamespace(
        id=11,
        organization=1,
        conversation_number=cn,
        parent_contact_uuid="par-1",
        contact_uuid="par-1",
        contact_identifier="+14155550000",
        crm_contact_id=crm,
        case_id=case,
        parent_channel=channel,
        source_direction_inbound=True,
    )


def _conv(status="open", state="agent_answered", channel="chat", conv_id=77, primary=203, parent=None, child_dir=True):
    parent = parent or _parent()
    return SimpleNamespace(
        id=conv_id,
        status=status,
        state=state,
        channel=channel,
        parent=parent.id,
        parent_conversation=parent,
        primary_aom=primary,
        primary_agent=SimpleNamespace(uuid=uuid_mod.UUID(AOM_UUID)),
        contact_uuid="par-1",
        customer_contact=None,
        sender_id=None,
        child_direction_inbound=child_dir,
        end_at=None,
        active_queue_entry_id=5,
    )


# ============================================================================ Pre matrix


# ============================================================================ retirement + route
