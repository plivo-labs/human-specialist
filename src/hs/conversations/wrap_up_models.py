from hs.infra.base_model import LenientModel


class _ChildParams(LenientModel):
    agent_sip_endpoint: str = ""
    agent_uuid: str = ""
    channel: str = ""
    contact_uuid: str = ""
    parent_contact_uuid: str = ""
    sender_id: str = ""
    release_agent_capacity: bool = False
    conversation_number: str = ""


class WrapUpConversationParams(_ChildParams):
    phone_or_email: str = ""
    direction: str = ""
    timeout_type: str = ""
    send_conversation_closed_event: bool = False


class ResolveConversationParams(_ChildParams):
    direction_inbound: bool = False
    contact_identifier: str = ""

    @classmethod
    def for_child(cls, conversation, agent_sip_endpoint, agent_uuid, **values) -> "ResolveConversationParams":
        """The resolve request for one child conversation, its identity read off the child and its parent."""
        parent = conversation.parent_conversation
        return cls(
            agent_sip_endpoint=agent_sip_endpoint,
            agent_uuid=str(agent_uuid),
            channel=conversation.channel,
            contact_uuid=conversation.contact_uuid,
            parent_contact_uuid=parent.parent_contact_uuid,
            conversation_number=parent.conversation_number,
            direction_inbound=bool(conversation.child_direction_inbound),
            sender_id=conversation.sender_id,
            contact_identifier=conversation.customer_contact,
            **values,
        )
