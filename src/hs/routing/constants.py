import re

NOT_FOUND = "not found"  # a lookup matched no row
CACHE_MISS = "not found in cache"  # a cache read found no entry
SIMULTANEOUS_ROUTING = "Simultaneous"
AVAILABILITY_ROUTING = "Availability"
CONVERSATIONS_HANDLED_ROUTING = "Lowest Conversations Handled"
CONVERSATIONS_HANDLE_TIME_ROUTING = "Lowest Conversation Handle Time"
STICKY_LOOSELY_BIND = "Loosely bind"
STICKY_STRICTLY_BIND = "Strictly bind"
STICKY_NO_STICKY_ROUTING = "No sticky routing"
CHANNEL_CALL = "call"
CHANNEL_CHAT = "chat"
CHANNEL_MESSAGE = "message"
CHANNEL_TASK = "task"
CHANNEL_CALLBACK = "callback"
CHANNEL_EMAIL = "email"
CONTACT_STATE_QUEUED = "QUEUED"
CONTACT_STATE_ACCEPTED = "ACCEPTED"
CONTACT_STATE_DROPPED = "DROPPED"
CONTACT_STATE_TIMEDOUT = "TIMEDOUT"
CONTACT_STATE_FAILED = "FAILED"
CONTACT_STATE_IN_TRANSIT = "IN_TRANSIT"
CONTACT_STATE_IN_REVERSE_ROUTING = "IN_REVERSE_ROUTING"
CONTACT_STATE_EXIT = "EXIT"
TIMEOUT_TYPE_SESSION = "session_timeout"
TRANSFER_MODE_WARM = "warm"
TRANSFER_MODE_COLD = "cold"
TRANSFER_MODE_BOTH = "both"
TRANSFER_TYPE_AGENT = "agent"
TRANSFER_TYPE_TEAM = "team"
TRANSFER_TYPE_EXTERNAL = "external"
TRANSFER_TYPE_WARM_EXTERNAL = "warm_external"
TRANSFER_TYPE_FLOW = "flow"
QNR_STATE_COMPLETED = "completed"
QNR_STATE_ENQUEUE_FAILED = "enqueue_failed"
QNR_STATE_TIMEOUT = "timeout"
QNR_STATE_ASSIGNMENT_FAILED = "assignment_failed"
QNR_STATE_QUEUE_EXIT = "queue_exit"
REVERSE_ROUTING = "reverse routing"
# the call results an unanswered queue stay ends with; the status map turns each into a disposition
UNANSWERED_QUEUE_TIMEOUT = "queue_timeout"
UNANSWERED_ASSIGNMENT_FAILED = "assignment_failed"
UNANSWERED_CALL_DROPPED = "abandoned"
UNANSWERED_QUEUE_EXIT = "queue_exit"


def hash_tag(auth_id: str) -> str:
    return f"{{{auth_id}}}."


def agent_capacity_key(auth_id: str, agent: str, channel: str) -> str:
    return f"{hash_tag(auth_id)}CAPACITY:{agent}:{channel}"


def conversation_capacity_base_key(auth_id: str, channel: str) -> str:
    return f"{hash_tag(auth_id)}CCK:{channel}"


def org_channel_unavailability_key(auth_id: str, channel: str) -> str:
    return f"{hash_tag(auth_id)}OCU:{channel}"


def reverse_routing_lock_key(agent_sip: str, conversation_id: str, channel: str) -> str:
    return f"RRLOCK:{agent_sip}:{conversation_id}:{channel}"


def contact_to_agent_mapping_key(contact_uuid: str) -> str:
    return f"CONTACT_AGENT_MAPPING:{contact_uuid}"


def queue_and_route_config_key(flow_run_uuid: str, run_uuid: str) -> str:
    return f"QUEUEANDROUTECONFIG:{flow_run_uuid}:{run_uuid}"


def queue_config_key(flow_run_uuid: str, run_uuid: str) -> str:
    return f"QUEUECONFIG:{flow_run_uuid}:{run_uuid}"


def transfer_in_progress_key(contact_uuid: str) -> str:
    return f"TIP:{contact_uuid}"


def mpc_to_be_primary_agent_key(qnr_run_uuid: str) -> str:
    return f"MTBPAK:{qnr_run_uuid}"


def contact_retry_key(flow_run_uuid: str, run_uuid: str) -> str:
    return f"CONTACTRETRY:{flow_run_uuid}:{run_uuid}"


def invited_agents_key(flow_run_uuid: str, run_uuid: str) -> str:
    return f"IA:{flow_run_uuid}:{run_uuid}"


# A skill filter's operator: the SQL comparison of an agent's level with the filter's value, which forward
# selection and reverse routing both apply verbatim; "" asks for the skill at any level.
SKILL_OPERATORS = ("", "=", "!=", "<>", "<", "<=", ">", ">=")


def offers_key(run_uuid: str) -> str:
    return f"OFFER:{run_uuid}"


def contact_params_key(contact_uuid: str) -> str:
    return f"CONTACT_PARAM:{contact_uuid}"


def queue_position_wait_time_key(node_uuid: str) -> str:
    return f"QPWT:{node_uuid}"


def sticky_agent_key(contact_identifier: str, queue_id=None, auth_id: str = "") -> str:
    """A contact's sticky agent in one queue (inbound), or with no queue the tenant's outbound one."""
    digits = re.sub(r"[^0-9]", "", contact_identifier or "")
    if queue_id is not None:
        return f"CASM:{digits}:{queue_id}"
    if not auth_id:
        raise ValueError("an outbound sticky key needs the tenant's auth id")
    return f"OCASM:{auth_id}:{digits}"


def create_sticky_routing_cache_seconds(no_expiry: bool, expiry_unit: str, expiry_value: int) -> int:
    if no_expiry:
        return 0
    if expiry_unit == "days":
        return expiry_value * 24 * 3600
    return expiry_value * 3600


EXPIRY_DURATION_IN_HOURS_CALL = 2
EXPIRY_DURATION_IN_HOURS_ASYNC = 25
EXPIRY_DURATION_IN_HOURS_CALLBACK = 75
EXPIRY_DURATION_IN_HOURS_SESSION_TASK = 360
TRANSFER_KEY_TTL_SECONDS = EXPIRY_DURATION_IN_HOURS_ASYNC * 3600
LEAVE_TYPE_LEAVE_PRIMARY = "leave_primary"
LEAVE_TYPE_DROP_SECONDARY = "drop_secondary"
LEAVE_TYPE_CLOSE_CONVERSATION = "close_conversation"
LEAVE_TYPE_LEAVE_SECONDARY = "leave_secondary"
SIGNAL_TRANSFER_INITIATED = "TRANSFER_INITIATED"
SIGNAL_CANCEL_TRANSFER = "CANCEL_TRANSFER"
SIGNAL_AGENT1_LEAVING = "AGENT1_LEAVING"
SIGNAL_AGENT2_DROPPED = "AGENT2_DROPPED"
SIGNAL_AGENT1_CLOSED_CONVERSATION = "AGENT1_CLOSED_CONVERSATION"
SIGNAL_TRANSFER_ACCEPTED = "TRANSFER_ACCEPTED"
SIGNAL_TRANSFER_SUCCESSFUL = "TRANSFER_SUCCESSFUL"
SIGNAL_TRANSFER_FAILED = "TRANSFER_FAILED"
TRANSFER_INITIATED_MSG = "Transfer initiated to <name>"
CANCEL_TRANSFER_MSG = "Transfer cancelled"
AGENT1_LEAVING_MSG = "<name> left the conversation"
AGENT2_DROPPED_MSG = "<name> dropped from the conversation"
AGENT1_CLOSED_CONVERSATION_MSG = "<name> closed the conversation"
TRANSFER_ACCEPTED_MSG = "Transfer accepted by <name>"
TRANSFER_FAILED_MSG = "Transfer request was not accepted"
TRANSFER_SUCCESSFUL_MSG = "Conversation transferred as agent transfer from <Agent1> to <Agent2>"
TRANSFER_SUCCESSFUL_MSG_TEAM = "Conversation transferred as team transfer from <Agent1> to <Agent2>"
TRANSFER_STATE_TRANSFER_FAILED = "transfer_failed"


def _channel_hours(contact_channel: str, task_hours: int) -> int:
    if contact_channel == CHANNEL_CALL:
        return EXPIRY_DURATION_IN_HOURS_CALL
    if contact_channel in (CHANNEL_CALLBACK, CHANNEL_TASK):
        return task_hours
    return EXPIRY_DURATION_IN_HOURS_CALLBACK if contact_channel == CHANNEL_MESSAGE else EXPIRY_DURATION_IN_HOURS_ASYNC


def get_cache_duration_for_contact_channel_session_flow(contact_channel: str) -> int:
    return _channel_hours(contact_channel, EXPIRY_DURATION_IN_HOURS_SESSION_TASK)


REVERSE_ROUTING_LOCK_TTL_SECONDS = 3600
# Defense-in-depth backstop TTL for the agent-capacity set. The CAPACITY set has no TTL, so a
# member leaked by ANY path (crash mid-flow, unhandled event ordering, a future bug) strands the agent
# unroutable forever. We put a TTL on the set on every successful acquire so a leak self-clears. It MUST be
# comfortably longer than the longest legitimate call + after-call-work, otherwise a live call could be
# freed mid-call → over-allocation. 4h (>> any realistic voice call/ACW).
AGENT_CAPACITY_TTL_SECONDS = 4 * 3600


def agent_capacity_ttl_seconds_for_channel(channel: str) -> int:
    """The backstop TTL, sized PER CHANNEL.

    The capacity set must outlive the longest legitimate conversation on its channel:
    conversations on longer-lived channels run for days, so a call-sized TTL lets a SATURATED agent's set expire mid-conversation —
    scard drops to 0 and routing over-allocates the agent, the exact inverse of the
    stranding this backstop guards against. Call keeps the 4h base; other channels use their
    session-flow lifetime + 1h margin (never below the base). A base of 0 disables the backstop."""
    if AGENT_CAPACITY_TTL_SECONDS <= 0:
        return 0
    if channel == CHANNEL_CALL:
        return AGENT_CAPACITY_TTL_SECONDS
    lifetime_hours = get_cache_duration_for_contact_channel_session_flow(channel)
    return max(AGENT_CAPACITY_TTL_SECONDS, (lifetime_hours + 1) * 3600)


RESERVATION_TIMEOUT_CACHE_TTL_SECONDS = 15 * 60
RESERVATION_TIMEOUT_DEFAULT_SECONDS = 30


def reservation_timeout_key(auth_id: str) -> str:
    return f"RESERVATIONTIMEOUT:{auth_id}"
