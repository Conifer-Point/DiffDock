import re

# Helper functions for dealing conforming to the Bioleap WS Apps message protocol
# which send packets like `<requestid> <cmdname> <message data>` in both directions.

def extractWsAppMessage(wholeMessage):
    """
    Bioleap WsApps messages look like `<requestid> <cmdname> <message data>`
    """
    match = re.match(r"^(#[a-z-_]+\d+) ([a-z-_]+) (.*)$", wholeMessage, re.DOTALL)
    if match:
        requestId, cmdName, message = match.groups()
        return requestId, cmdName, message
    else:
        return None, None, wholeMessage

def formWsAppMessage(requestId, cmdName, message=""):
    return ' '.join([part for part in [requestId, cmdName, message] if part])

def formCompletedMessage(requestId):
    return formWsAppMessage(requestId, 'completed')
