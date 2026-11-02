from hs.conversations.wrap_up_models import ResolveConversationParams, WrapUpConversationParams
from hs.infra.log import logger
from hs.routing import constants as rc


def handle_wrap_up_voice(params: WrapUpConversationParams) -> str | None:
    from hs.voice.hangup import voice_handle_wrap_up

    return voice_handle_wrap_up(params)


def resolve_conversation(params: ResolveConversationParams) -> None:
    try:
        if params.channel == rc.CHANNEL_CALL:
            from hs.voice.hangup import voice_handle_resolve

            voice_handle_resolve(params)
    except Exception as e:
        logger.error(f"resolve_conversation: error channel={params.channel} contact={params.contact_uuid}: {e}")
