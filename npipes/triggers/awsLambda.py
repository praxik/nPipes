# -*- mode: python;-*-

from ..message.header import Message
from ..outcome import Outcome, Success, Failure
from ..message.ezqconverter import toEzqOrJsonLines

import boto3


def sendMessage(name, message:Message) -> Outcome[str, None]:
    """Directly invokes the named Lambda function with message as the payload

    Invoking a Lambda function directly is orthogonal to running an nPipes
    processor *on* top of Lambda. The Lambda function invoked here doesn't have
    to be running nPipes; as long as it is minimally aware of the nPipes message
    structure, it could (for example) simply discard the header and process the
    message body.
    """
    client = boto3.client("lambda")
    resp = client.invoke(FunctionName=name,
                         InvocationType="Event",
                         Payload=toEzqOrJsonLines(message).encode("utf-8"))
    # Boto3 docs specify the following success codes based on InvocationType:
    # RequestResponse => 200, Event => 202, DryRun => 204
    # We're forcing Event invocation here, so...
    if resp["StatusCode"] == 202:
        return Success(None)
    else:
        return Failure(f"Lambda invocation failure; function: {name}; error: {resp['FunctionError']}")
