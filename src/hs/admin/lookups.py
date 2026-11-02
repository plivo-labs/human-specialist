from hs.admin.account import AccountOrgMapping
from hs.admin.account_models import AOMFullModel
from hs.conversations.models import ConversationCustomStatusMappingModel
from hs.infra.constants import BEFORE_TRANSFER_STATE, CONVERSATION_CUSTOM_STATE_CACHE_SUFFIX
from hs.infra.context import context_aom_uuid, context_org_name, context_org_uuid
from hs.infra.helpers import build_cache_key_without_integration, is_valid_uuid
from hs.infra.redis import Cache


def build_key_for_conversation_custom_state(org_id: int) -> str:
    return "{" + str(org_id) + "}" + f"{CONVERSATION_CUSTOM_STATE_CACHE_SUFFIX}"


def fetch_aom_by_uuid_or_sip_and_set_context(agent_sip_or_uuid: str, build_full_model=True) -> AOMFullModel:
    aom: AOMFullModel = fetch_aom_by_uuid_or_sip(agent_sip_or_uuid, build_full_model)
    if aom:
        context_org_name.set(aom.org.name)
        context_org_uuid.set(aom.org.uuid)
        context_aom_uuid.set(aom.uuid)
    return aom


def fetch_aom_by_uuid_or_sip(agent_sip_or_uuid: str, build_full_model=True) -> AOMFullModel:
    aom: AOMFullModel = None
    if is_valid_uuid(agent_sip_or_uuid):
        aom = AccountOrgMapping.get_by_uuid(agent_sip_or_uuid, build_full_model)
    else:
        aom = AccountOrgMapping.get_by_sip_endpoint(agent_sip_or_uuid, build_full_model)
    return aom


def cache_custom_states(org_id: int, states: list):
    cache_key = build_key_for_conversation_custom_state(org_id)
    cache = Cache.get_instance()
    for state in states:
        cache.hset(cache_key, state.state, state.model_dump_json())


def cached_custom_state(org_id: int, state_name: str):
    import json

    value = Cache.get_instance().hget(build_key_for_conversation_custom_state(org_id), state_name)
    if value:
        return ConversationCustomStatusMappingModel(**json.loads(value))
    return None


def fetch_org_by_id_and_set_context(org_id):
    from hs.admin.account import Organization

    org = Organization.get_by_id(org_id)
    if not org:
        return f"Org not found with id : {org_id}", None
    context_org_name.set(org.name)
    context_org_uuid.set(org.uuid)
    return "", org


class BeforeTransferStateCache:
    @staticmethod
    def _get_key(contact_uuid, channel):

        return build_cache_key_without_integration(BEFORE_TRANSFER_STATE, contact_uuid, channel)

    @classmethod
    def get(cls, contact_uuid, channel):
        from hs.infra.log import logger

        cache_key = cls._get_key(contact_uuid, channel)
        logger.info(f"in BeforeTransferStateCache: getting cache key: {cache_key}")
        return Cache.get_instance().get(cache_key)

    @classmethod
    def set(cls, contact_uuid, channel, state):
        from hs.infra.log import logger

        cache_key = cls._get_key(contact_uuid, channel)
        logger.info(f"in BeforeTransferStateCache: setting cache key: {cache_key}, state: {state}")
        return Cache.get_instance().set(cache_key, state)

    @classmethod
    def delete(cls, contact_uuid, channel):
        from hs.infra.log import logger

        cache_key = cls._get_key(contact_uuid, channel)
        logger.info(f"in BeforeTransferStateCache: deleting cache key: {cache_key}")
        return Cache.get_instance().delete(cache_key)


class ApiLockManager:
    def __init__(self, api_name, api_run_id, unique_identifier, expiry_in_seconds=600):

        self.cache_key = build_cache_key_without_integration(
            identifier_name="RL", unique_id=unique_identifier, entity_name=api_name
        )
        self.expiry_in_seconds = expiry_in_seconds
        self.api_run_id = api_run_id

    def acquire_lock_for_api(self) -> bool:
        res = Cache.get_instance().set(
            self.cache_key, self.api_run_id, nx=True, expiry_in_seconds=self.expiry_in_seconds
        )
        if not res:
            return None
        return Cache.get_instance().get(self.cache_key) == self.api_run_id


def fetch_org_by_auth_id(org_auth_id: str):
    from hs.admin.account import Organization

    org = Organization.get_org_by_auth_id(org_auth_id)
    if not org:
        return f"Org not found with auth_id : {org_auth_id}", None
    return "", org


def fetch_org_by_auth_id_and_set_context(org_auth_id: str):
    error, org = fetch_org_by_auth_id(org_auth_id)
    if error:
        return error, org
    context_org_name.set(org.name)
    context_org_uuid.set(org.uuid)
    return "", org


def is_valid_aom_user_for_chunk(aom_uuid: str):
    from hs.admin.account import AccountOrgMapping
    from hs.infra.context import context_org_uuid

    if not is_valid_uuid(aom_uuid):
        return False
    aom = AccountOrgMapping.get_by_uuid(aom_uuid, build_full_model=True)
    if not aom:
        return False
    org_uuid = context_org_uuid.get()
    return not (org_uuid and str(org_uuid) != str(aom.org.uuid))
