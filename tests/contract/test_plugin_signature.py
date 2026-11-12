import secrets
import uuid

import pytest
from fastapi import APIRouter, Depends
from fastapi.testclient import TestClient
from sqlalchemy import insert

from hs import app as service
from hs import core, plivo
from tests.unit.test_app import plugins  # noqa: F401

TOKEN = "sim-auth-token-0123456789"


@pytest.fixture
def tenant(db_engine):
    row = {
        "name": "Acme",
        "plivo_auth_id": f"SA{secrets.token_hex(9).upper()}",
        "plivo_token_enc": plivo.encrypt(TOKEN),
    }
    with core.tx() as db:
        return db.execute(insert(core.T.tenant).values(**row).returning(*core.T.tenant.c)).one()


def test_a_plugin_route_on_a_tenant_path_accepts_a_v3_signed_request_and_refuses_an_unsigned_one(plugins, tenant):  # noqa: F811
    router = APIRouter()
    router.post("/acme/{tenant_id}/continue")(lambda t=Depends(plivo.voice_signed): {"tenant": str(t.id)})
    plugins({"acme": lambda hs: hs.app.include_router(router)})
    path, form, nonce = f"/acme/{tenant.id}/continue", {"CallUUID": "cu-1"}, uuid.uuid4().hex
    signature = plivo.v3(TOKEN, core.settings.public_base_url + path, [], list(form.items()), nonce)
    signed = {"X-Plivo-Signature-V3": signature, "X-Plivo-Signature-V3-Nonce": nonce}
    with TestClient(service.app) as client:
        assert client.post(path, data=form, headers=signed).json() == {"tenant": str(tenant.id)}
        assert client.post(path, data=form).status_code == 401
