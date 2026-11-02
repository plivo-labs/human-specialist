MPC_VOICE_EVENT_KEY = "MPCVE"
VALUE_EXISTS_ERROR = "value already exists"
LATER_EVENT_FOUND_ERROR = "later event found"
# Tags a Plivo error that is an outage (no answer, or a 5xx), not a refusal: the call routes answer 503.
PLIVO_UNAVAILABLE = "plivo unavailable"
REPEATABLE_EVENTS = {
    "ParticipantHold",
    "ParticipantUnhold",
    "ParticipantSpeakStopped",
    "ParticipantAudioStopped",
    "ParticipantDigitInput",
}
CALLBACK_CONFIG = "#rc=5&ct=2000&rp=5xx,ct"
EXPIRY_DURATION_IN_HOURS_CALL = 2
EXPIRY_DURATION_IN_HOURS_ASYNC = 25
MPC_ROLE_CUSTOMER = "customer"
EXIT_WAIT_TIMEOUT = "Member Wait Timeout Triggered"  # a member's ParticipantExitCause when its waitTime ran out
MPC_STATUS_CALLBACK_EVENTS = (
    "mpc-state-changes,participant-state-changes,add-participant-api-events,"
    "participant-audio-events,participant-digit-input-events"
)
MPC_INITIALISE_EVENT = "MPCInitialise"
MPC_INITIALISED_CALLBACK_EVENT = "MPCInitialized"
MPC_START_EVENT = "MPCStart"
MPC_END_EVENT = "MPCEnd"
MPC_PARTICIPANT_JOIN_EVENT = "ParticipantJoin"
MPC_ADD_PARTICIPANT_COMPLETED_EVENT = "AddParticipantByAPIActionCompleted"
MPC_PARTICIPANT_EXIT_EVENT = "ParticipantExit"
MPC_ADD_PARTICIPANT_INITIATED_EVENT = "AddParticipantByAPIActionInitiated"
MPC_PARTICIPANT_SPEAK_STOPPED_EVENT = "ParticipantSpeakStopped"
MPC_PARTICIPANT_AUDIO_STOPPED_EVENT = "ParticipantAudioStopped"
MPC_PARTICIPANT_DIGIT_INPUT_EVENT = "ParticipantDigitInput"
MPC_RECORDING_COMPLETED_EVENT = "MPCRecordingCompleted"
CALL_INITIATED_EVENT = "Initiated"
CALL_RINGING_EVENT = "Ringing"
CALL_ANSWERED_EVENT = "Answered"
CALL_HANGUP_EVENT = "Hangup"
PARTICIPANT_SPECIFIC_EVENTS = {
    MPC_PARTICIPANT_JOIN_EVENT,
    MPC_PARTICIPANT_EXIT_EVENT,
    MPC_ADD_PARTICIPANT_COMPLETED_EVENT,
    MPC_ADD_PARTICIPANT_INITIATED_EVENT,
    CALL_ANSWERED_EVENT,
    CALL_RINGING_EVENT,
    CALL_HANGUP_EVENT,
}
MIN_ESTIMATED_WAIT_TIME_VALUE = "{{queue.min_estimated_wait_time}}"
MAX_ESTIMATED_WAIT_TIME_VALUE = "{{queue.max_estimated_wait_time}}"
ESTIMATED_WAIT_TIME_VALUE = "{{queue.estimated_wait_time}}"
QUEUE_POSITION_VALUE = "{{queue.position}}"
WAIT_TIME_PATTERNS = (MIN_ESTIMATED_WAIT_TIME_VALUE, MAX_ESTIMATED_WAIT_TIME_VALUE, ESTIMATED_WAIT_TIME_VALUE)
ESTIMATED_WAIT_INTERVAL_HOURS = 3
MAX_ENTRIES_FOR_WAIT_TIME = 10
MPC_AGENT_STATUS_CALLBACK_EVENTS = "mpc-state-changes,participant-state-changes,add-participant-api-events"
MPC_ROLE_AGENT = "agent"
MPC_ROLE_SUPERVISOR = "supervisor"
MPC_ROLE_AGENT_TITLE = "Agent"
HEADER_PARTICIPANT_EXISTS_VALUE = "true"
SECONDARY_STATE_MONITOR = "monitor"
PARTICIPANT_TYPE_ADD_PARTICIPANT = "add_participant"
PARTICIPANT_TYPE_SUPERVISOR = "supervisor_participant"
PARTICIPANT_TYPE_EXTERNAL = "external_transfer"
CALL_RESERVATION_MAX_TIMEOUT_MINUTES = 5
RESTRICTED = "restricted"
SECONDARY_STATE_BARGE = "barge"
SECONDARY_STATE_MONITOR_PAUSED = "monitor_paused"
MONITOR_EVENT_SUPERVISOR_PRESENT = "SUPERVISOR_PRESENT"
SUPERVISOR_LEG = "SUPERVISOR_LEG"  # the supervisor's own leg on a call, by call and supervisor
SUPERVISOR_LEAVE_REQUESTED = "SupervisorLeaveRequested"  # a call event per leg: its leave already ran
MONITOR_EVENT_CONVERSATION_NOT_ACCESSIBLE = "CONVERSATION_NOT_ACCESSIBLE"
SUPERVISOR_LEFT_MSG = "left the conversation"
PARTICIPANT_STATE_SETTINGS = {
    "monitor": (True, True),
    "monitor_visible": (True, True),
    "coach": (True, False),
    SECONDARY_STATE_BARGE: (False, False),
}
MAX_SECONDARY_PARTICIPANTS = 2
MAX_PARTICIPANT_ALIAS_LEN = 20
MPC_END_INITIATED = "MPCEndInitiated"
HANGUP_CODE_CALL_CANCELLED_BY_API = "1000"
HANGUP_CODE_NORMAL_HANGUP = "4000"
HANGUP_CODE_MPC_ENDED = "4020"
HANGUP_CODES_NO_REVERSE_ROUTING = ("3010", "2020")
MONITOR_EVENT_CONVERSATION_CLOSED = "CONVERSATION_CLOSED"
TRANSFER_SUCCESSFUL_MSG_EXTERNAL = "Conversation transferred as external transfer from <Agent1> to <Agent2>"
