GENERIC_ERROR_RESPONSE: dict = {
    "AGENT_NO_CAPACITY_ERROR_RESPONSE": {
        "category": "Agent",
        "agentAction": True,
        "message": "Agent Capacity Issue",
        "description": "You have reached your open conversation limit. Wrap-up the open conversations to continue.",
        "errorType": "Whole conversation",
        "comments": "Message sending failed. Try again later.",
    },
    "ERROR_CHILD_CONVERSATION_EXISTS": {
        "category": "Service",
        "agentAction": True,
        "message": "Conversation already exist",
        "description": "There is an active conversation with this user for this channel. Continue the conversation in the active conversation or try with an alternate 'from or to' number.",
        "errorType": "Whole conversation",
        "comments": "",
    },
}
